from __future__ import annotations

import csv
import json
from pathlib import Path
from xml.sax.saxutils import escape

import duckdb

from fraudledger.detect.engine import RULE_BLURB
from fraudledger.hashing import sha256_file, sha256_text
from fraudledger.settings import Settings

BANNED = ("fraudster", "guilty", "stole", "confirmed fraud", "fraud amount")

DISCLAIMER = (
    "Statistical flags are review leads, not findings of fraud. "
    "Excess dollars are a gap versus a peer median, not a loss amount and not a recoverable amount. "
    "Public aggregates cannot show whether a service was appropriate or delivered. "
    "A human decides whether any lead is worth a tip."
)


def _esc(value: object) -> str:
    return escape("" if value is None else str(value))


def _svg(points: list[tuple[str, float]]) -> str:
    if not points:
        return ""
    width, height, pad = 520, 140, 16
    values = [value for _, value in points]
    low, high = min(values), max(values)
    span = high - low or 1
    coords = []
    for index, value in enumerate(values):
        x = pad + (width - 2 * pad) * (index / max(len(values) - 1, 1))
        y = height - pad - (height - 2 * pad) * ((value - low) / span)
        coords.append(f"{x:.1f},{y:.1f}")
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
        f'role="img" aria-label="Monthly paid amounts">'
        f'<polyline fill="none" stroke="#0f766e" stroke-width="2" points="{" ".join(coords)}"/>'
        f"</svg>"
    )


def _finding_block(name: str, key: str, score: float, tier: str, rules: str, excess: float, flags: list) -> str:
    lines = [
        f"{tier}  {name}  ({key})",
        f"Score {score:.1f}. Rules: {rules or 'none'}.",
        f"Sum of peer-gap estimates: ${excess:,.0f}." if excess else "No dollar peer-gap on these rules.",
    ]
    for rule_id, metric, threshold, peer, dollars, detail in flags:
        metric_text = f"{float(metric):.4g}" if metric is not None else "n/a"
        threshold_text = f"{float(threshold):.4g}" if threshold is not None else "n/a"
        lines.append(
            f"- {rule_id}: metric {metric_text} vs threshold {threshold_text}"
            + (f", peer {float(peer):.4g}" if peer is not None else "")
            + (f", peer-gap ${float(dollars):,.0f}" if dollars else "")
        )
        lines.append(f"  {detail}")
    return "\n".join(lines)


def write_report(con: duckdb.DuckDBPyConnection, settings: Settings, run_id: str) -> Path:
    run_dir = settings.data_dir / "runs" / run_id
    evidence = run_dir / "evidence"
    evidence.mkdir(parents=True, exist_ok=True)
    demo_only = con.execute(
        """
        SELECT
            COUNT(*) > 0
            AND COUNT(*) = COUNT(*) FILTER (WHERE origin = 'demo')
        FROM facts_medicaid
        """
    ).fetchone()[0]

    findings = con.execute(
        """
        SELECT s.provider_key, p.name, p.city, p.state, s.score, s.tier, s.rules, s.excess_dollars
        FROM scores s
        LEFT JOIN providers p ON p.provider_key = s.provider_key
        WHERE s.run_id = ? AND s.tier IN ('Critical', 'High', 'Elevated')
        ORDER BY s.score DESC, s.provider_key
        """,
        [run_id],
    ).fetchall()

    text_parts = [
        "Fraud Ledger review leads",
        f"Run {run_id}",
        DISCLAIMER,
        "",
    ]
    if demo_only:
        text_parts.append(
            "DEMONSTRATION DATA. Providers use fictional NPIs in the 199999xxxx range. They are not real entities."
        )
        text_parts.append("")
    text_parts.append("Methodology")
    text_parts.append(f"Config SHA-256: {settings.config_sha256}")
    text_parts.append(f"Peer group: national HCPCS median. A code is used only when at least {settings.min_peer_n} providers billed it.")
    text_parts.append("Rules are not mutually exclusive. One provider can trip several at once.")
    for rule_id, blurb in RULE_BLURB.items():
        cfg = settings.rules.get(rule_id) or {}
        weight = cfg.get("weight", "")
        text_parts.append(f"{rule_id} (weight {weight}): {blurb} Settings: {json.dumps(cfg, sort_keys=True)}")
    text_parts.append("")
    text_parts.append("Findings")

    html_findings = []
    for key, name, city, state, score, tier, rules, excess in findings:
        if not key:
            continue
        flags = con.execute(
            """
            SELECT rule_id, metric_value, threshold, peer_value, excess_dollars, detail_json
            FROM flags
            WHERE run_id = ? AND provider_key = ?
            ORDER BY rule_id
            """,
            [run_id, key],
        ).fetchall()
        monthly = con.execute(
            """
            SELECT month, SUM(paid) AS paid
            FROM facts_medicaid
            WHERE billing_npi = ?
            GROUP BY 1
            ORDER BY 1
            """,
            [key],
        ).fetchall()
        evidence_path = evidence / f"{key}.csv"
        with evidence_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["month", "hcpcs", "claim_lines", "beneficiaries", "paid"])
            detail_rows = con.execute(
                """
                SELECT month, hcpcs, claim_lines, beneficiaries, paid
                FROM facts_medicaid
                WHERE billing_npi = ?
                ORDER BY month, hcpcs
                LIMIT 240
                """,
                [key],
            ).fetchall()
            writer.writerows(detail_rows)
        display = str(name or key)
        place = ", ".join(part for part in (city, state) if part)
        text_parts.append("")
        text_parts.append(_finding_block(display, key, score, tier, rules, excess or 0, flags))
        rows_html = "".join(
            "<li><code>{}</code> metric {}, threshold {}{}</li>".format(
                _esc(rule_id),
                f"{float(metric):.4g}" if metric is not None else "n/a",
                f"{float(threshold):.4g}" if threshold is not None else "n/a",
                f", peer-gap ${float(dollars):,.0f}" if dollars else "",
            )
            for rule_id, metric, threshold, _peer, dollars, _detail in flags
        )
        html_findings.append(
            f"""
            <article class="finding tier-{_esc(tier).lower()}">
              <header>
                <span class="badge">{_esc(tier)}</span>
                <h3>{_esc(display)}</h3>
                <p class="meta">{_esc(key)} · {_esc(place)} · score {float(score or 0):.1f}</p>
              </header>
              <p>Rules: {_esc(rules or "")}. Peer-gap estimate ${float(excess or 0):,.0f}.</p>
              {_svg([(str(month), float(paid or 0)) for month, paid in monthly])}
              <ul>{rows_html}</ul>
            </article>
            """
        )

    clusters = con.execute(
        """
        SELECT cluster_id, provider_count, owner_count, flagged_count, score, excess_dollars
        FROM clusters
        WHERE run_id = ?
        ORDER BY score DESC
        """,
        [run_id],
    ).fetchall()
    text_parts.append("")
    text_parts.append("Ownership clusters")
    for cluster_id, providers, owners, flagged, score, dollars in clusters:
        members = [
            row[0]
            for row in con.execute(
                "SELECT provider_key FROM cluster_members WHERE run_id = ? AND cluster_id = ? ORDER BY 1",
                [run_id, cluster_id],
            ).fetchall()
        ]
        text_parts.append(
            f"Cluster {cluster_id}: {providers} providers, {owners} owners, {flagged} flagged, "
            f"score {score:.1f}, peer-gap ${dollars or 0:,.0f}. Members: {', '.join(members)}"
        )

    narrative = "\n".join(text_parts) + "\n"
    for banned in BANNED:
        if banned in narrative.lower():
            raise RuntimeError(f"Report text contains banned phrase: {banned}")
    (run_dir / "report.txt").write_text(narrative, encoding="utf-8")
    (run_dir / "config.snapshot.yaml").write_text(settings.config_text, encoding="utf-8")
    summary = {
        "run_id": run_id,
        "demo_only": bool(demo_only),
        "finding_count": len(findings),
        "cluster_count": len(clusters),
        "config_sha256": settings.config_sha256,
        "disclaimer": DISCLAIMER,
    }
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    html = _html(run_id, bool(demo_only), html_findings, DISCLAIMER, settings.config_sha256)
    if any(phrase in html.lower() for phrase in BANNED):
        raise RuntimeError("HTML report contains a banned phrase")
    (run_dir / "report.html").write_text(html, encoding="utf-8")
    _pdf(run_dir / "report.pdf", narrative)
    _manifest(con, run_dir, run_id, settings)
    return run_dir


def _html(run_id: str, demo: bool, findings: list[str], disclaimer: str, config_hash: str) -> str:
    banner = (
        "<p class='banner'>Demonstration data only. Fictional NPIs in the 199999xxxx range. Not real entities.</p>"
        if demo
        else ""
    )
    body = "\n".join(findings) or "<p>No elevated leads in this run.</p>"
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Fraud Ledger {escape(run_id)}</title>
  <style>
    body {{ margin: 0; background: #f4f1ea; color: #1c1917; font: 16px/1.5 Georgia, serif; }}
    main {{ max-width: 860px; margin: 0 auto; padding: 32px 20px 80px; }}
    h1 {{ font-weight: 500; letter-spacing: -0.03em; }}
    .disclaimer, .banner {{ background: #fff; border-left: 4px solid #0f766e; padding: 12px 16px; }}
    .banner {{ border-left-color: #a16207; }}
    .finding {{ background: #fff; margin: 16px 0; padding: 16px 18px; }}
    .badge {{ font: 12px/1 ui-sans-serif, sans-serif; letter-spacing: 0.08em; text-transform: uppercase; }}
    .tier-critical .badge {{ color: #9f1239; }}
    .tier-high .badge {{ color: #c2410c; }}
    .tier-elevated .badge {{ color: #a16207; }}
    .meta {{ color: #57534e; font: 14px/1.4 ui-sans-serif, sans-serif; }}
    code {{ font-family: ui-monospace, monospace; }}
  </style>
</head>
<body>
<main>
  <p class="meta">Fraud Ledger</p>
  <h1>Review leads</h1>
  <p class="meta">Run {escape(run_id)} · config {escape(config_hash[:16])}</p>
  <p class="disclaimer">{escape(disclaimer)}</p>
  {banner}
  {body}
</main>
</body>
</html>
"""


def _pdf(path: Path, narrative: str) -> None:
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer

    styles = getSampleStyleSheet()
    doc = SimpleDocTemplate(str(path), pagesize=letter, title="Fraud Ledger review leads")
    story = []
    for block in narrative.split("\n\n"):
        safe = escape(block).replace("\n", "<br/>")
        story.append(Paragraph(safe, styles["BodyText"]))
        story.append(Spacer(1, 8))
    doc.build(story)


def _manifest(con: duckdb.DuckDBPyConnection, run_dir: Path, run_id: str, settings: Settings) -> None:
    artifacts = []
    for path in sorted(run_dir.rglob("*")):
        if not path.is_file() or path.name == "MANIFEST.json":
            continue
        relative = path.relative_to(run_dir).as_posix()
        artifacts.append({"path": relative, "sha256": sha256_file(path), "bytes": path.stat().st_size})
    manifest = {
        "run_id": run_id,
        "config_sha256": settings.config_sha256,
        "disclaimer": DISCLAIMER,
        "artifacts": artifacts,
    }
    encoded = json.dumps(manifest, indent=2)
    manifest_path = run_dir / "MANIFEST.json"
    manifest_path.write_text(encoded, encoding="utf-8")
    digest = sha256_text(encoded)
    con.execute("DELETE FROM run_artifacts WHERE run_id = ?", [run_id])
    con.executemany(
        "INSERT INTO run_artifacts (run_id, path, sha256, kind) VALUES (?, ?, ?, ?)",
        [(run_id, item["path"], item["sha256"], "file") for item in artifacts]
        + [(run_id, "MANIFEST.json", digest, "manifest")],
    )


def verify_run(run_dir: Path) -> None:
    manifest_path = run_dir / "MANIFEST.json"
    if not manifest_path.is_file():
        raise SystemExit(f"No manifest at {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    problems = []
    for item in manifest.get("artifacts") or []:
        path = run_dir / item["path"]
        if not path.is_file():
            problems.append(f"missing {item['path']}")
            continue
        actual = sha256_file(path)
        if actual != item["sha256"]:
            problems.append(f"hash mismatch {item['path']}")
    if problems:
        raise SystemExit("verify-run failed: " + "; ".join(problems))
