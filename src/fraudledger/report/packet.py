"""One provider's report packet. A person submits it; this module does not."""

from __future__ import annotations

import csv
import io
import zipfile
from pathlib import Path

import duckdb

from fraudledger.detect.engine import RULE_BLURB
from fraudledger.report.assemble import BANNED, DISCLAIMER, _pdf
from fraudledger.reviews import PROVIDER_KEY
from fraudledger.settings import Settings

INTAKE = "https://america.gov/fraud"


def _reject(text: str) -> None:
    lowered = text.lower()
    for phrase in BANNED:
        if phrase in lowered:
            raise RuntimeError(f"Report packet contains banned phrase: {phrase}")


def build_packet(
    con: duckdb.DuckDBPyConnection,
    settings: Settings,
    run_id: str,
    provider_key: str,
) -> Path:
    if not PROVIDER_KEY.match(provider_key or ""):
        raise SystemExit(f"Provider key is not a safe identifier: {provider_key!r}")
    if not run_id or "/" in run_id or "\\" in run_id:
        raise SystemExit("Refusing that run id.")
    row = con.execute(
        "SELECT demo_only, status FROM runs WHERE run_id = ?",
        [run_id],
    ).fetchone()
    if row is None:
        raise SystemExit(f"No run {run_id}.")
    if row[1] != "succeeded":
        raise SystemExit(f"Run {run_id} did not succeed.")
    if row[0]:
        raise SystemExit(
            "This run only contains demonstration data. "
            "Load the public working set before preparing a report packet."
        )
    present = con.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM facts_medicaid WHERE billing_npi = ?)
            + (SELECT COUNT(*) FROM scores WHERE run_id = ? AND provider_key = ?)
        """,
        [provider_key, run_id, provider_key],
    ).fetchone()[0]
    if not present:
        raise SystemExit(f"{provider_key} is not in this run.")

    identity = con.execute(
        """
        SELECT name, entity_type, taxonomy, address, city, state, zip, enumeration_date, origin
        FROM providers WHERE provider_key = ?
        """,
        [provider_key],
    ).fetchone()
    score = con.execute(
        """
        SELECT score, tier, rules, excess_dollars, flag_count
        FROM scores WHERE run_id = ? AND provider_key = ?
        """,
        [run_id, provider_key],
    ).fetchone()
    flags = con.execute(
        """
        SELECT rule_id, metric_value, threshold, peer_value, excess_dollars, detail_json
        FROM flags
        WHERE run_id = ? AND provider_key = ?
        ORDER BY rule_id
        """,
        [run_id, provider_key],
    ).fetchall()
    span = con.execute(
        """
        SELECT MIN(month), MAX(month), SUM(paid), SUM(claim_lines), COUNT(*)
        FROM facts_medicaid
        WHERE billing_npi = ?
        """,
        [provider_key],
    ).fetchone()
    evidence_rows = con.execute(
        """
        SELECT month, hcpcs, claim_lines, beneficiaries, paid
        FROM facts_medicaid
        WHERE billing_npi = ?
        ORDER BY month, hcpcs
        LIMIT 8000
        """,
        [provider_key],
    ).fetchall()
    edges = con.execute(
        """
        SELECT owner_key, owner_name, role_code, percentage, edge_weight, origin
        FROM ownership_edges
        WHERE provider_key = ?
        ORDER BY edge_weight DESC, owner_key
        """,
        [provider_key],
    ).fetchall()
    leie = con.execute(
        """
        SELECT name, excl_type, excl_date, origin
        FROM leie
        WHERE npi = ?
        ORDER BY excl_date
        """,
        [provider_key],
    ).fetchall()
    snapshots = con.execute(
        """
        SELECT source_id, url, retrieved_at, sha256, byte_count
        FROM snapshots
        ORDER BY retrieved_at, source_id
        """
    ).fetchall()

    name = (identity[0] if identity and identity[0] else "") or provider_key
    place_bits = []
    if identity:
        place_bits = [part for part in (identity[3], identity[4], identity[5], identity[6]) if part]
    lines = [
        "Fraud Ledger report packet",
        "",
        DISCLAIMER,
        "",
        "This file is independent analysis of public data. It is a review lead.",
        "It is not a determination that a crime occurred, and it was not sent to any agency.",
        "You submit it yourself, if you decide it is worth sending, at " + INTAKE,
        "The Criminal Tip Rewards Pilot is discretionary. This tool does not compute a reward.",
        "",
        "Provider",
        f"Name: {name}",
        f"NPI or provider key: {provider_key}",
    ]
    if identity:
        lines.append(f"Entity: {identity[1] or 'unknown'} · taxonomy {identity[2] or 'unknown'}")
        lines.append("Address: " + (", ".join(place_bits) if place_bits else "not in the directory extract"))
        lines.append(f"Enumeration date: {identity[7] or 'unknown'} · directory source {identity[8] or 'unknown'}")
    else:
        lines.append("No directory row. The key is known from billing facts only.")
    lines.append("")
    lines.append(f"Run {run_id}")
    lines.append(f"Config SHA-256: {settings.config_sha256}")
    if score:
        lines.append(
            f"Review tier {score[1]}. Score {score[0]:.1f}. "
            f"{score[4]} indicator{'s' if score[4] != 1 else ''}: {score[2] or 'none'}."
        )
        if score[3]:
            lines.append(
                f"Sum of peer-gap estimates: ${score[3]:,.0f}. "
                "That is the gap versus a peer median, not a loss amount and not a recoverable amount. "
                "Do not enter it on an intake form as money taken."
            )
        else:
            lines.append("These indicators do not carry a dollar peer-gap.")
    else:
        lines.append("No score was stored for this provider in this run.")
    if span and span[4]:
        lines.append(
            f"Loaded billing rows: {span[4]} from {span[0]} through {span[1]}. "
            f"Sum of paid amounts in the loaded extract: ${span[2] or 0:,.0f}. "
            f"Sum of claim lines: {span[3] or 0:,.0f}."
        )
        if span[4] > len(evidence_rows):
            lines.append(f"The attached evidence table is the first {len(evidence_rows)} rows, not every row.")
    else:
        lines.append("No billing rows for this key are in the loaded extract.")
    lines.append("")
    lines.append("Indicators")
    if not flags:
        lines.append("No indicator rows were stored for this provider in this run.")
    for rule_id, metric, threshold, peer, dollars, detail in flags:
        blurb = RULE_BLURB.get(rule_id, "")
        lines.append(
            f"{rule_id}: {blurb} "
            f"Metric {metric:.4g}, threshold {threshold:.4g}"
            + (f", peer {peer:.4g}" if peer is not None else "")
            + (f", peer-gap ${dollars:,.0f}" if dollars else "")
            + "."
        )
        if detail:
            lines.append(f"  Detail: {detail}")
    lines.append("")
    lines.append("Public exclusion list")
    if not leie:
        lines.append("No LEIE row for this NPI was loaded.")
    for excluded_name, excl_type, excl_date, origin in leie:
        lines.append(
            f"LEIE name {excluded_name or 'blank'}, type {excl_type or 'blank'}, "
            f"date {excl_date or 'blank'}, source {origin}."
        )
    lines.append("")
    lines.append("Ownership edges kept from the public enrollment files")
    if not edges:
        lines.append("None for this provider key.")
    for owner_key, owner_name, role, percent, weight, origin in edges:
        lines.append(
            f"Owner {owner_key} {owner_name or ''} role {role or 'blank'} "
            f"percent {percent if percent is not None else 'blank'} "
            f"weight {weight:.2f} source {origin}."
        )
    lines.append("")
    lines.append("Files this run retrieved")
    if not snapshots:
        lines.append("No snapshot rows. The extract may have been loaded from a local path.")
    for source_id, url, retrieved, digest, nbytes in snapshots:
        lines.append(f"{source_id} retrieved {retrieved} bytes {nbytes} sha256 {digest}")
        lines.append(f"  {url}")
    lines.append("")
    lines.append("How a person files this")
    lines.append(f"1. Open {INTAKE} and use the signed intake. This program cannot file it for you.")
    lines.append("2. Paste the narrative below the provider heading into the description of what you observed.")
    lines.append("3. Say that the source is your analysis of named public files, and attach report.pdf.")
    lines.append("4. If the form asks for an amount, do not type the peer-gap estimate as money taken. Leave the amount blank or say the attachment shows a peer-median gap.")
    lines.append("5. Keep your own copy of this zip. The intake may allow only a few attachments, so prefer report.pdf plus narrative.txt.")
    narrative = "\n".join(lines) + "\n"
    _reject(narrative)

    evidence_buf = io.StringIO()
    writer = csv.writer(evidence_buf)
    writer.writerow(["month", "hcpcs", "claim_lines", "beneficiaries", "paid"])
    writer.writerows(evidence_rows)
    evidence_csv = evidence_buf.getvalue()
    _reject(evidence_csv)

    indicator_buf = io.StringIO()
    indicator_writer = csv.writer(indicator_buf)
    indicator_writer.writerow(["rule_id", "metric", "threshold", "peer", "peer_gap", "detail"])
    indicator_writer.writerows(flags)
    indicator_csv = indicator_buf.getvalue()
    _reject(indicator_csv)

    submit = "\n".join(
        [
            "Do this yourself. Fraud Ledger does not contact the government.",
            f"Intake: {INTAKE}",
            "Attach report.pdf. You can also attach narrative.txt if the form accepts a text file.",
            "The peer-gap figure is not a loss amount. Do not present it as money taken.",
            "A reward, if any, is discretionary and is not calculated here.",
            "",
        ]
    )
    _reject(submit)

    dest_dir = settings.data_dir / "exports"
    dest_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = dest_dir / f"{run_id}-{provider_key}.pdf"
    _pdf(pdf_path, narrative)
    dest = dest_dir / f"{run_id}-{provider_key}.zip"
    with zipfile.ZipFile(dest, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("narrative.txt", narrative)
        archive.writestr("evidence.csv", evidence_csv)
        archive.writestr("indicators.csv", indicator_csv)
        archive.writestr("how-to-submit.txt", submit)
        archive.write(pdf_path, "report.pdf")
    pdf_path.unlink(missing_ok=True)
    return dest
