"""One report for every provider in an ownership group. A person submits it; this module does not."""

from __future__ import annotations

import csv
import io
import zipfile
from pathlib import Path

import duckdb

from fraudledger.detect.score import unduplicated_excess
from fraudledger.report.assemble import DISCLAIMER
from fraudledger.report.chart import chart_for, paid_drawing
from fraudledger.report.packet import (
    _explain_flag,
    _heading_for,
    _month,
    _owner_lines,
    _reject,
    _usd,
    _weight_sentence,
    _write_pdf,
    _zip,
    group_concern,
)
from fraudledger.reference import role_label
from fraudledger.settings import Settings


def _percent(value: object) -> str:
    if value is None:
        return "CMS stored no percent"
    return f"{float(value):g}%"


def _place(address: object, city: object, state: object, zip_code: object) -> str:
    city_state = ", ".join(part for part in (city, state) if part)
    tail = " ".join(part for part in (city_state, _zip(zip_code)) if part)
    return ", ".join(part for part in (address, tail) if part)


def _is_po_box(address: str) -> bool:
    text = (address or "").upper()
    return "PO BOX" in text or "P.O. BOX" in text or "POST OFFICE BOX" in text


def _identity_line(row: tuple) -> str:
    _key, name, entity, taxonomy, address, city, state, zip_code, enumerated, formed = row[:10]
    kind = entity or "not loaded"
    if kind.isalpha() and kind == kind.lower():
        kind = kind[:1].upper() + kind[1:]
    place = _place(address, city, state, zip_code)
    parts = [
        f"NPPES entity type: {kind}.",
        f"NPPES taxonomy {taxonomy}. That is the specialty code, not a finding." if taxonomy else "",
        f"Directory address: {place}." if place else "No street address was loaded for this provider.",
        (
            f"NPPES enumeration date: {enumerated}. That is the date the NPI was issued, not an incorporation date."
            if enumerated
            else "NPPES enumeration date: not loaded."
        ),
        (
            f"Incorporation date: {formed}. That date is from the enrollment file. It is not a Medicare enrollment date."
            if formed
            else "Incorporation date: not loaded."
        ),
    ]
    return " ".join(part for part in parts if part)


def build_group_packet(
    con: duckdb.DuckDBPyConnection,
    settings: Settings,
    run_id: str,
    cluster_id: int,
) -> Path:
    if not run_id or "/" in run_id or "\\" in run_id:
        raise SystemExit("Refusing that run id.")
    if not isinstance(cluster_id, int) or isinstance(cluster_id, bool):
        raise SystemExit("Group id must be a number.")
    row = con.execute(
        "SELECT status FROM runs WHERE run_id = ?",
        [run_id],
    ).fetchone()
    if row is None:
        raise SystemExit(f"No run {run_id}.")
    if row[0] != "succeeded":
        raise SystemExit(f"Run {run_id} did not succeed.")
    header = con.execute(
        """
        SELECT provider_count, owner_count, flagged_count
        FROM clusters WHERE run_id = ? AND cluster_id = ?
        """,
        [run_id, cluster_id],
    ).fetchone()
    if header is None:
        raise SystemExit(f"No group {cluster_id} in run {run_id}.")

    members = con.execute(
        """
        SELECT m.provider_key, COALESCE(p.name, ''), p.entity_type, p.taxonomy,
               p.address, p.city, p.state, p.zip,
               p.enumeration_date, p.incorporation_date,
               s.score, s.tier, s.rules, s.flag_count
        FROM cluster_members m
        LEFT JOIN providers p ON p.provider_key = m.provider_key
        LEFT JOIN scores s ON s.run_id = m.run_id AND s.provider_key = m.provider_key
        WHERE m.run_id = ? AND m.cluster_id = ?
        ORDER BY s.score DESC NULLS LAST, p.name, m.provider_key
        """,
        [run_id, cluster_id],
    ).fetchall()
    if not members:
        raise SystemExit(f"Group {cluster_id} has no providers.")

    flags = con.execute(
        """
        SELECT f.provider_key, f.rule_id, f.metric_value, f.threshold, f.peer_value,
               f.excess_dollars, f.detail_json
        FROM flags f
        JOIN cluster_members m
          ON m.provider_key = f.provider_key AND m.run_id = f.run_id
        WHERE f.run_id = ? AND m.cluster_id = ?
        ORDER BY f.provider_key, f.rule_id
        """,
        [run_id, cluster_id],
    ).fetchall()
    edges = con.execute(
        """
        SELECT e.provider_key, e.owner_key, e.owner_name, e.role_code, e.percentage
        FROM ownership_edges e
        JOIN cluster_members m ON m.provider_key = e.provider_key
        WHERE m.run_id = ? AND m.cluster_id = ?
        ORDER BY e.owner_key, e.provider_key, e.role_code
        """,
        [run_id, cluster_id],
    ).fetchall()
    spans = {
        key: rest
        for key, *rest in con.execute(
            """
            SELECT f.billing_npi, MIN(f.month), MAX(f.month), SUM(f.paid), SUM(f.claim_lines), COUNT(*)
            FROM facts_medicaid f
            JOIN cluster_members m ON m.provider_key = f.billing_npi
            WHERE m.run_id = ? AND m.cluster_id = ?
            GROUP BY f.billing_npi
            """,
            [run_id, cluster_id],
        ).fetchall()
    }
    monthly: dict[str, list] = {}
    for npi, month, paid, codes in con.execute(
        """
        SELECT f.billing_npi, f.month, SUM(f.paid), string_agg(DISTINCT upper(trim(f.hcpcs)), ',')
        FROM facts_medicaid f
        JOIN cluster_members m ON m.provider_key = f.billing_npi
        WHERE m.run_id = ? AND m.cluster_id = ?
        GROUP BY 1, 2
        ORDER BY 1, 2
        """,
        [run_id, cluster_id],
    ).fetchall():
        monthly.setdefault(npi, []).append((month, paid, codes))

    by_flag: dict[str, list] = {}
    for provider_key, rule_id, metric, threshold, peer, dollars, detail in flags:
        by_flag.setdefault(provider_key, []).append((rule_id, metric, threshold, peer, dollars, detail))
    by_edge: dict[str, list] = {}
    owners: dict[str, dict] = {}
    for provider_key, owner_key, owner_name, role, percent in edges:
        by_edge.setdefault(provider_key, []).append((owner_key, owner_name, role, percent, None, None))
        slot = owners.setdefault(
            owner_key or "unknown",
            {"name": "", "providers": {}},
        )
        shown = (owner_name or "").strip()
        if len(shown) > len(slot["name"]):
            slot["name"] = shown
        slot["providers"].setdefault(provider_key, []).append((role, percent))
    shared_owner_keys = {key for key, slot in owners.items() if len(slot["providers"]) >= 2}
    names = {row[0]: (row[1] or row[0]) for row in members}
    gaps = {
        key: unduplicated_excess((rule_id, dollars) for rule_id, _m, _t, _p, dollars, _d in rows)
        for key, rows in by_flag.items()
    }
    combined = sum(gaps.values())
    flagged = [row for row in members if row[10] is not None]
    quiet = [row for row in members if row[10] is None]
    owner_bits = [
        f"{owners[key]['name'] or 'Unnamed owner'} (PECOS associate id {key})"
        for key in sorted(shared_owner_keys, key=lambda key: (-len(owners[key]["providers"]), owners[key]["name"] or key))
    ]
    place_counts: dict[tuple, int] = {}
    place_label: dict[tuple, str] = {}
    for member in members:
        street = (member[4] or "").strip()
        if not street:
            continue
        key = (street.upper(), (member[5] or "").strip().upper(), (member[6] or "").strip().upper())
        place_counts[key] = place_counts.get(key, 0) + 1
        place_label.setdefault(key, _place(street, member[5], member[6], ""))
    shared_labels = [place_label[key] for key, count in place_counts.items() if count >= 2]
    if not shared_labels:
        address_line = "No two providers in the group use the same street address."
    elif len(shared_labels) == 1:
        address_line = f"A shared directory address is {shared_labels[0]}."
    else:
        address_line = f"Shared directory addresses include {shared_labels[0]} and {len(shared_labels) - 1} more."
    leaders = [
        f"{names[member[0]]} (NPI {member[0]}) at about {_usd(gap)}"
        for gap, member in sorted(
            ((gaps.get(member[0]) or 0, member) for member in flagged),
            key=lambda item: -item[0],
        )
        if gap
    ]

    blocks: list[tuple[str, str]] = [
        ("title", f"Group {cluster_id}"),
        ("small", f"Run {run_id}  |  {len(members)} providers"),
        ("body", "Review lead from public data. Not a finding that a crime occurred. This file was not sent to any agency."),
        ("heading", "What raised your concern"),
        ("body", "Paste the next paragraph into that box. It names the group, the link, the amounts, where the records came from, and what is still unknown."),
        ("body", group_concern(
            cluster_id,
            len(members),
            len(flagged),
            combined,
            owner_bits,
            address_line,
            leaders,
        )),
        ("body", "This one file covers every provider in the group. You do not need a separate report for each provider to show how they are connected."),
        ("heading", "How to read this file"),
        ("body", "Read it in this order. The first paragraph answers the form. Then the bottom line for the whole group. Then the owners and street addresses that link the providers. Then the list of every provider. Then a full section for each provider that has a billing indicator, including its own dollars and its own chart. Last, the providers with no billing indicator, with the address and the owners CMS recorded, so the organization is complete."),
        ("heading", "Bottom line up front"),
        ("body", (
            f"Group {cluster_id} has {len(members)} providers. "
            f"{len(flagged)} have at least one billing indicator. "
            f"{len(quiet)} do not. "
            "The group number is a label, not a rank and not a severity score. "
            "Do not add the provider scores together."
        )),
        ("body", (
            f"Dollars above a typical peer, added across the providers that have a gap: {_usd(combined) if combined else 'none'}. "
            "A payment jump is counted once inside each provider. "
            "The dollars of different providers are then added so the group can be read in one place. "
            "That sum is not one payment, not a loss amount, and not money taken."
        )),
    ]

    blocks.append(("heading", "Owners on more than one provider"))
    blocks.append((
        "body",
        "A PECOS associate id is column ASSOCIATE ID - OWNER in the CMS owner file. It is not an NPI. "
        "The same id on two providers is the owner link. "
        "Direct owner (CMS role 34) is equity in that provider. "
        "Indirect owner (CMS role 35) is equity in a company that owns that provider. "
        "Officer (CMS role 40), director (CMS role 41), and managing employee are control relationships. "
        "The percent on a control row is often the same stake already listed. Do not add those percents. "
        "CMS stored no percent means the cell was blank. It does not mean zero.",
    ))
    if not shared_owner_keys:
        blocks.append(("body", "No PECOS associate id is on more than one provider in this group. A shared owner is not the link."))
    for owner_key in sorted(shared_owner_keys, key=lambda key: (-len(owners[key]["providers"]), owners[key]["name"] or key)):
        slot = owners[owner_key]
        who = slot["name"] or "Unnamed owner"
        blocks.append((
            "body",
            f"{who} (PECOS associate id {owner_key}) is on {len(slot['providers'])} providers in this group.",
        ))
        for provider_key, roles in sorted(slot["providers"].items(), key=lambda item: (names.get(item[0], item[0]), item[0])):
            relationship = "; ".join(
                f"{role_label(role)} at {_percent(percent)}" for role, percent in roles
            )
            blocks.append(("body", f"{names.get(provider_key, provider_key)} (NPI {provider_key}): {relationship}."))

    blocks.append(("heading", "Street addresses on more than one provider"))
    blocks.append((
        "body",
        "The address is the directory address on the provider row. "
        f"An address is used as a group link only when 3 to {int(settings.address_link_max)} providers in the directory use that same street, city, and state. "
        "A post-office box is never used as a link.",
    ))
    places: dict[tuple, dict] = {}
    for member in members:
        street = (member[4] or "").strip()
        if not street:
            continue
        key = (street.upper(), (member[5] or "").strip().upper(), (member[6] or "").strip().upper())
        slot = places.setdefault(key, {"street": street, "city": member[5] or "", "state": member[6] or "", "providers": []})
        slot["providers"].append(member[0])
    shared_places = [slot for slot in places.values() if len(slot["providers"]) >= 2]
    if not shared_places:
        blocks.append(("body", "No street address is used by two providers in this group. A shared address is not the link."))
    for slot in sorted(shared_places, key=lambda item: (-len(item["providers"]), item["street"])):
        shown = _place(slot["street"], slot["city"], slot["state"], "")
        named = ", ".join(f"{names.get(key, key)} (NPI {key})" for key in sorted(slot["providers"], key=lambda key: (names.get(key, key), key)))
        if _is_po_box(slot["street"]):
            blocks.append((
                "body",
                f"{shown} is a post-office box on {named}. It was not used as a group link.",
            ))
            continue
        directory_count = int(con.execute(
            """
            SELECT COUNT(*) FROM providers
            WHERE upper(trim(address)) = upper(trim(?))
              AND upper(trim(coalesce(city, ''))) = upper(trim(?))
              AND upper(trim(coalesce(state, ''))) = upper(trim(?))
            """,
            [slot["street"], slot["city"] or "", slot["state"] or ""],
        ).fetchone()[0])
        qualified = 3 <= directory_count <= int(settings.address_link_max)
        rule = (
            f"{directory_count} providers in the directory use that street, city, and state. "
            + ("This address meets the link rule." if qualified else "This address does not meet the link rule, so it was not used as a link.")
        )
        blocks.append(("body", f"{shown} is the directory address of {named}. {rule}"))

    blocks.append(("heading", "Every provider in this group"))
    for index, member in enumerate(members, start=1):
        provider_key = member[0]
        gap = gaps.get(provider_key) or 0
        if member[10] is None:
            standing = "No billing indicator."
        else:
            standing = (
                f"Review tier: {member[11]}. Provider score: {float(member[10]):.1f}. "
                f"Indicators: {member[12] or 'none'}. "
                f"Dollars above a typical peer: {_usd(gap) if gap else 'none'}."
            )
        place = _place(member[4], member[5], member[6], member[7]) or "no street address loaded"
        blocks.append(("body", f"{index}. {names[provider_key]} (NPI {provider_key}). {place}. {standing}"))

    drawings: dict = {}
    blocks.append(("heading", "Providers with a billing indicator"))
    if not flagged:
        blocks.append(("body", "No provider in this group has a billing indicator. The sections below are the providers that share an owner or an address only."))
    else:
        blocks.append((
            "body",
            "Each section is one provider. An indicator is a comparison with a peer or a public list. "
            "It is not a finding that a crime occurred. "
            "A cost comparison uses other billers in the same state when that state has enough billers of the procedure code. Otherwise it uses the national median. "
            "The chart is paid by month for that provider. The solid line is the provider. The dashed line is the peer median.",
        ))
    for member in flagged:
        provider_key = member[0]
        blocks.append(("heading", f"{names[provider_key]}"))
        blocks.append(("body", f"NPI {provider_key}. {_identity_line(member)}"))
        rule_ids = [part for part in str(member[12] or "").split(",") if part]
        gap = gaps.get(provider_key) or 0
        blocks.append((
            "body",
            f"Review tier: {member[11]}. Provider score: {float(member[10]):.1f}. "
            f"Dollars above a typical peer: {_usd(gap) if gap else 'none'}. "
            "That figure is this provider only. It is not a loss amount and not money taken.",
        ))
        blocks.append(("body", _weight_sentence(settings, rule_ids, float(member[10]))))
        span = spans.get(provider_key)
        if span and span[4]:
            blocks.append((
                "body",
                f"Paid in the loaded Medicaid extract: {_usd(span[2])}. "
                f"Months: {_month(span[0])} through {_month(span[1])}. "
                f"Billing rows kept: {int(span[4]):,}. Claim lines: {float(span[3] or 0):,.0f}. "
                "This is the sum of the rows kept for the priority procedure codes. It is not a complete payment history.",
            ))
        else:
            blocks.append(("body", "No billing rows for this provider are in the loaded extract."))
        chart = chart_for(con, settings, provider_key, member[6], monthly.get(provider_key, []))
        if chart.get("points"):
            drawings[provider_key] = paid_drawing(chart["points"], chart["benchmark"], chart["peer_label"])
            blocks.append(("heading", "Paid by month"))
            blocks.append(("chart", provider_key))
        blocks.append(("body", chart["note"]))
        provider_flags = by_flag.get(provider_key, [])
        if not provider_flags:
            blocks.append(("body", "No indicator rows were stored for this provider in this run."))
        for rule_id, metric, threshold, peer, dollars, detail in provider_flags:
            blocks.append(("heading", _heading_for(rule_id, detail)))
            blocks.append(("body", _explain_flag(rule_id, metric, threshold, peer, dollars, detail)))
        _append_ownership(blocks, provider_key, by_edge.get(provider_key, []), shared_owner_keys, owners, names)

    blocks.append(("heading", "Providers with no billing indicator"))
    if not quiet:
        blocks.append(("body", "Every provider in this group has a billing indicator. There is no extra member to list."))
    else:
        blocks.append((
            "body",
            "These providers did not stand out on billing. They are in the file so the organization can be read without another report. "
            "A missing indicator is not a statement that the billing is ordinary in every respect. It means none of the stored comparisons fired.",
        ))
    for member in quiet:
        provider_key = member[0]
        blocks.append(("heading", names[provider_key]))
        blocks.append(("body", f"NPI {provider_key}. {_identity_line(member)} No billing indicator was stored in this run."))
        _append_ownership(blocks, provider_key, by_edge.get(provider_key, []), shared_owner_keys, owners, names)

    blocks.append(("heading", "Where the figures come from"))
    blocks.append(("small", f"Config SHA-256 {settings.config_sha256}. The thresholds travel with the run."))
    snapshots = con.execute(
        "SELECT source_id, url, retrieved_at, sha256, byte_count FROM snapshots ORDER BY retrieved_at, source_id"
    ).fetchall()
    if not snapshots:
        blocks.append(("body", "No download snapshot was stored. The extract may have been loaded from a local path."))
    for source_id, url, retrieved, digest, nbytes in snapshots:
        blocks.append(("small", f"{source_id} retrieved {retrieved}. {nbytes} bytes. SHA-256 {digest}. {url}"))
    blocks.append(("body", DISCLAIMER))

    narrative = []
    for kind, text in blocks:
        if kind == "chart":
            continue
        if kind in {"title", "heading"}:
            narrative.append("")
        narrative.append(text)
    narrative_text = "\n".join(narrative).strip() + "\n"
    _reject(narrative_text)

    member_buf = io.StringIO()
    member_writer = csv.writer(member_buf)
    member_writer.writerow([
        "npi", "name", "entity_type", "address", "city", "state", "zip",
        "enumeration_date", "incorporation_date", "tier", "provider_score",
        "indicators", "dollars_above_typical_peer", "billing_indicator",
    ])
    for member in members:
        member_writer.writerow([
            member[0], member[1], member[2] or "", member[4] or "", member[5] or "",
            member[6] or "", member[7] or "", member[8] or "", member[9] or "",
            member[11] or "", "" if member[10] is None else f"{float(member[10]):.1f}",
            member[12] or "", f"{gaps.get(member[0], 0):.2f}",
            "yes" if member[10] is not None else "no",
        ])
    member_csv = member_buf.getvalue()
    _reject(member_csv)

    owner_buf = io.StringIO()
    owner_writer = csv.writer(owner_buf)
    owner_writer.writerow([
        "pecos_associate_id", "owner_name", "provider_npi", "provider_name",
        "relationship", "percent", "on_another_provider_in_this_group",
    ])
    for provider_key, owner_key, owner_name, role, percent in edges:
        owner_writer.writerow([
            owner_key, owner_name or "", provider_key, names.get(provider_key, provider_key),
            role_label(role), "" if percent is None else percent,
            "yes" if (owner_key or "unknown") in shared_owner_keys else "no",
        ])
    owner_csv = owner_buf.getvalue()
    _reject(owner_csv)

    indicator_buf = io.StringIO()
    indicator_writer = csv.writer(indicator_buf)
    indicator_writer.writerow(["provider_npi", "provider_name", "rule_id", "peer_gap", "explanation"])
    for provider_key, rule_id, metric, threshold, peer, dollars, detail in flags:
        indicator_writer.writerow([
            provider_key,
            names.get(provider_key, provider_key),
            rule_id,
            "" if dollars is None else dollars,
            _explain_flag(rule_id, metric, threshold, peer, dollars, detail),
        ])
    indicator_csv = indicator_buf.getvalue()
    _reject(indicator_csv)

    dest_dir = settings.data_dir / "exports"
    dest_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = dest_dir / f"{run_id}-group-{cluster_id}.pdf"
    _write_pdf(pdf_path, blocks, drawings)
    dest = dest_dir / f"{run_id}-group-{cluster_id}.zip"
    with zipfile.ZipFile(dest, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("narrative.txt", narrative_text)
        archive.writestr("members.csv", member_csv)
        archive.writestr("owners.csv", owner_csv)
        archive.writestr("indicators.csv", indicator_csv)
        archive.write(pdf_path, "report.pdf")
    pdf_path.unlink(missing_ok=True)
    return dest


def _append_ownership(blocks, provider_key, edges, shared_owner_keys, owners, names) -> None:
    blocks.append(("heading", "Ownership on this provider"))
    lines = _owner_lines(edges)
    if not lines:
        blocks.append(("body", "No ownership rows were kept for this provider."))
    else:
        blocks.extend(("body", line) for line in lines)
    shared_here = []
    for owner_key, owner_name, _role, _percent, _weight, _origin in edges:
        key = owner_key or "unknown"
        if key in shared_owner_keys and key not in shared_here:
            shared_here.append(key)
    if shared_here:
        bits = []
        for key in shared_here:
            others = [
                f"{names.get(other, other)} (NPI {other})"
                for other in owners[key]["providers"]
                if other != provider_key
            ]
            who = owners[key]["name"] or "Unnamed owner"
            bits.append(f"{who} (PECOS associate id {key}) is also on " + ", ".join(others))
        blocks.append((
            "body",
            "These associate ids are the link to other providers in this group: " + ". ".join(bits) + ".",
        ))
    elif lines:
        blocks.append(("body", "None of these PECOS associate ids are on another provider in this group."))
