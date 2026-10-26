"""One provider's report packet. A person submits it; this module does not."""

from __future__ import annotations

import csv
import io
import json
import zipfile
from pathlib import Path

import duckdb

from fraudledger.detect.engine import RULE_BLURB
from fraudledger.detect.score import R11_BONUS, unduplicated_excess
from fraudledger.report.assemble import BANNED, DISCLAIMER, _esc
from fraudledger.report.chart import chart_for, paid_drawing
from fraudledger.graph.links import explain_membership
from fraudledger.reference import role_label
from fraudledger.reviews import PROVIDER_KEY
from fraudledger.settings import Settings

INTAKE = "https://america.gov/fraud"


def _reject(text: str) -> None:
    lowered = text.lower()
    for phrase in BANNED:
        if phrase in lowered:
            raise RuntimeError(f"Report packet contains banned phrase: {phrase}")


def _usd(value: object) -> str:
    if value is None or value == "":
        return "unknown"
    number = float(value)
    if abs(number) >= 100:
        return f"${number:,.0f}"
    return f"${number:,.2f}"


def _count(value: object) -> str:
    if value is None or value == "":
        return "unknown"
    return f"{float(value):,.0f}"


def _plain_number(value: object) -> str:
    if value is None or value == "":
        return "unknown"
    number = float(value)
    if abs(number - round(number)) < 0.05:
        return f"{number:,.0f}"
    return f"{number:,.1f}"


def _percent(value: object) -> str:
    if value is None or value == "":
        return "unknown"
    return f"{float(value) * 100:.0f}%"


def _month(value: object) -> str:
    if value is None or value == "":
        return "unknown"
    text = str(value)[:10]
    if len(text) >= 7 and text[4] == "-":
        return text[:7]
    return text


def _zip(value: object) -> str:
    text = "" if value is None else str(value).strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) == 9 and digits == text:
        return f"{digits[:5]}-{digits[5:]}"
    return text


def _as_dict(raw: object) -> dict:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


_TITLE = {
    "R01": "Paid per claim line is high",
    "R02": "Several procedure codes are in the top 10 percent",
    "R03": "Many claim lines per patient",
    "R04": "Many claim lines per working day",
    "R05": "A few procedure codes account for a large paid total",
    "R06": "Monthly paid amounts barely move",
    "R07": "Paid jumped more than the patient count explains",
    "R08": "The latest year grew sharply",
    "R09": "The first year of billing is large for a new organization",
    "R10": "A recent organization has a very large paid total",
    "R11": "The billing NPI is on the exclusion list",
    "R12": "A payment jump lines up with a new owner",
    "R13": "An owner matches the exclusion list",
    "R14": "Medicare hospice stays are long",
    "R15": "Medicare home-health visits per episode are high",
    "R16": "A servicing NPI is on the exclusion list",
    "R17": "One servicing NPI is paid through several billing NPIs",
    "R18": "Medicare live discharges are high",
    "R19": "Medicare home-health readmissions are high",
}


_CLAUSE = {
    "R01": "paid per claim line sits above the peer median",
    "R02": "more than one procedure code is in the top 10 percent of rates",
    "R03": "claim lines per patient are above the cap",
    "R04": "claim lines per working day are above the cap",
    "R05": "one or two procedure codes account for a very large paid total",
    "R06": "monthly paid amounts barely move",
    "R07": "paid jumped from one year to the next by more than the patient count explains",
    "R08": "the latest year grew sharply against the prior year",
    "R09": "the first year of billing is large and a real organization date says the organization is new",
    "R10": "lifetime paid in this extract is very large and the organization date is recent",
    "R11": "the billing NPI is on the OIG exclusion list and payment falls on or after the exclusion date",
    "R12": "a payment jump lines up with a newly associated owner",
    "R13": "an owner matches the exclusion list on name and state",
    "R14": "Medicare hospice stays are long relative to other hospices",
    "R15": "Medicare home-health visits per episode are high relative to other agencies",
    "R16": "a servicing NPI, not the billing NPI, is on the exclusion list",
    "R17": "one servicing NPI is paid through several billing NPIs that do not share an owner",
    "R18": "Medicare Care Compare live discharges are high relative to other hospices",
    "R19": "Medicare Care Compare readmissions are high relative to other home-health agencies",
}


def _heading_for(rule_id: str, detail: object) -> str:
    payload = _as_dict(detail)
    if rule_id == "R01" and payload.get("hcpcs"):
        return f"{rule_id}. Procedure code {payload['hcpcs']}"
    if rule_id in {"R18", "R19"} and payload.get("measure"):
        return f"{rule_id}. {payload['measure']}"
    return f"{rule_id}. {_TITLE.get(rule_id, 'Indicator')}"


def _patients(payload: dict) -> str:
    current = payload.get("patients")
    prior = payload.get("prior_patients")
    if current is None or prior is None:
        return (
            "Average monthly patient counts were not in the extract, "
            "so this jump was not set aside for a matching rise in patients."
        )
    change = payload.get("patient_growth")
    if change is None:
        moved = f"was {_count(current)} in the later year and {_count(prior)} in the prior year"
    else:
        moved = f"went from {_count(prior)} to {_count(current)} ({_percent(change)})"
    return (
        f"The average monthly patient count {moved}. "
        "That figure is an average of the monthly beneficiary totals, not a count of unique people for the year. "
        "The change in patients is not large enough to explain the paid jump, and paid per patient did not stay flat."
    )


def _year_core(payload: dict, threshold, dollars) -> str:
    year = payload.get("year") or "the later year"
    prior_year = payload.get("prior_year") or "the prior year"
    paid = payload.get("paid")
    prior_paid = payload.get("prior_paid")
    change = payload.get("absolute_change")
    ratio = ""
    if paid not in (None, "") and prior_paid not in (None, "", 0):
        times = float(paid) / float(prior_paid)
        ratio = f" {year} was {times:.1f} times {prior_year}."
    cutoff = ""
    if threshold not in (None, ""):
        cutoff = (
            f" The cutoff is a later year more than {float(threshold) + 1:.1f} times the prior year."
        )
    dollars_text = ""
    if change not in (None, ""):
        dollars_text = f" The increase in this extract is {_usd(change)}."
    elif dollars:
        dollars_text = f" Peer-gap estimate {_usd(dollars)}."
    return (
        f"Paid in {year} was {_usd(paid)}. Paid in {prior_year} was {_usd(prior_paid)}."
        + ratio
        + cutoff
        + dollars_text
    )


def _year_jump(payload: dict, threshold, dollars) -> str:
    return _year_core(payload, threshold, dollars) + " " + _patients(payload)


def _explain_flag(rule_id: str, metric, threshold, peer, dollars, detail: object) -> str:
    payload = _as_dict(detail)
    if rule_id == "R01":
        scope = payload.get("peer_scope") or "peer"
        state = payload.get("state") or ""
        if scope == "state" and state:
            where = f"{state} median"
        elif scope == "national":
            where = "national median"
        else:
            where = "peer median"
        gap = ""
        if dollars:
            gap = (
                f" Dollars above a typical peer on this code: (rate - median) x lines = {_usd(dollars)}. "
                "That figure is the gap versus the median. It is not money taken."
            )
        return (
            f"Paid per claim line was {_usd(payload.get('rate'))}. "
            f"The {where} for this code was {_usd(payload.get('median'))}. "
            f"The state median is used only when the state has enough billers of the code. This row used the {where}. "
            f"Claim lines: {_count(payload.get('lines'))}. Paid on this code in the extract: {_usd(payload.get('paid'))}."
            + gap
        )
    if rule_id == "R02":
        found = payload.get("codes_above_p90")
        if found is None:
            found = metric
        return (
            f"{_count(found)} procedure codes were priced above the 90th percentile of peers. "
            f"The cutoff is {_count(threshold)} codes. "
            "A single high-priced code is a different indicator. This one is the count of codes in the top 10 percent. "
            "The peer group for each code is the state when that state has enough billers, and the national median otherwise."
        )
    if rule_id == "R03":
        ratio = payload.get("lines_per_max_monthly_beneficiaries")
        if ratio is None:
            ratio = metric
        return (
            f"Claim lines per patient in the busiest month were {_plain_number(ratio)}. "
            f"The cutoff is {_plain_number(threshold)}. "
            "Patient counts are not added across months, because the same person can appear in more than one month. "
            "The denominator is the largest single month."
        )
    if rule_id == "R04":
        lines = payload.get("lines_per_working_day")
        if lines is None:
            lines = metric
        days = payload.get("working_days_per_month")
        day_text = f" The count assumes {_count(days)} working days in each month that has a billing row." if days else ""
        return (
            f"This provider averaged {_plain_number(lines)} claim lines per working day. "
            f"The cutoff is {_plain_number(threshold)}."
            + day_text
        )
    if rule_id == "R05":
        return (
            f"{_count(payload.get('distinct_hcpcs'))} procedure codes account for {_usd(payload.get('paid'))}. "
            f"The cutoff is at most {_count(threshold)} codes, once paid is large enough. "
            "A provider that bills only the expected codes for hospice, home health, or personal care is not flagged for this pattern. "
            "A narrow code list does not show that a service was inappropriate."
        )
    if rule_id == "R06":
        return (
            f"Paid totaled {_usd(payload.get('paid'))} across {_count(payload.get('months'))} months. "
            f"Month-to-month variation was {_percent(payload.get('cv'))} of the average month. "
            f"The cutoff for a flat pattern is variation under {_percent(threshold)}. "
            "Demand usually moves more than that. A flat pattern on only the expected codes for one service is not flagged."
        )
    if rule_id == "R07":
        return (
            _year_jump(payload, threshold, dollars)
            + " A jump that mostly tracks more patients, or that leaves paid per patient almost unchanged, is not flagged."
        )
    if rule_id == "R08":
        return (
            _year_core(payload, threshold, dollars)
            + " This is the sharp-growth test on the latest year. It is the same jump as the payment-jump indicator, with a higher cutoff. "
            + _patients(payload)
            + " A jump that mostly tracks more patients is not flagged."
        )
    if rule_id == "R09":
        formed = payload.get("incorporation_date")
        enumerated = payload.get("enumeration_date")
        if formed:
            dated = (
                f"Incorporation date {formed}. That date is not a Medicare enrollment date. "
                "It falls in the first year of billing or the year before."
            )
        elif enumerated:
            dated = (
                f"No incorporation date was loaded. The NPPES enumeration date is {enumerated}, "
                "and that year is the first year of billing in this extract."
            )
        else:
            dated = "No organization date was stored on this row."
        return (
            f"The first year in this extract is {payload.get('first_year') or 'unknown'}, "
            f"with {_usd(payload.get('first_year_paid'))} paid. The floor is {_usd(threshold)}. "
            + dated
            + " A missing incorporation date and a missing enumeration date do not count as new. "
            "An older incorporation date keeps this indicator from firing."
        )
    if rule_id == "R10":
        formed = payload.get("incorporation_date")
        enumerated = payload.get("enumeration_date")
        cutoff = payload.get("enumeration_on_or_after") or "the cutoff"
        if formed:
            dated = (
                f"Incorporation date {formed}, which is on or after {cutoff}. "
                "That date is not a Medicare enrollment date. When an incorporation date is present, it controls this indicator."
            )
        elif enumerated:
            dated = (
                f"No incorporation date was loaded. The NPPES enumeration date {enumerated} is on or after {cutoff}, "
                "so the enumeration date was used instead."
            )
        else:
            dated = "No organization date was stored on this row."
        return (
            f"Paid in this extract is {_usd(payload.get('paid'))}. The floor is {_usd(threshold)}. "
            + dated
        )
    if rule_id == "R11":
        kind = payload.get("excl_type")
        kind_text = f" Exclusion type: {kind}." if kind else ""
        return (
            f"The billing NPI is on the exclusion list as of {payload.get('excl_date') or 'an unknown date'}. "
            f"Paid on or after that date in this extract: {_usd(payload.get('paid_on_or_after_exclusion'))}."
            + kind_text
            + " Payment on or after the exclusion date is why this indicator is in the Critical tier on its own."
        )
    if rule_id == "R12":
        years = payload.get("owner_years") or []
        listed = ", ".join(str(year) for year in years) if years else "an unknown year"
        return (
            _year_core(payload, threshold, dollars)
            + f" An owner was associated in {listed}. "
            "The indicator requires that association in the jump year or the year before. "
            "The increase is counted once, on the payment-jump indicator. It is not added again here. "
            "The association date is not proof that the owner caused the jump."
        )
    if rule_id == "R13":
        paid = payload.get("paid")
        paid_text = f" Paid in this extract for the billing NPI: {_usd(paid)}." if paid not in (None, "") else ""
        return (
            f"Owner {payload.get('owner_first') or ''} {payload.get('owner_last') or ''} "
            f"in {payload.get('owner_state') or 'an unknown state'} matches the exclusion list "
            f"as of {payload.get('excl_date') or 'an unknown date'}. "
            "The match uses first name, last name, and state, and the middle name when both sides have one. "
            "The agency name was not compared. A reinstated person is not flagged."
            + paid_text
        )
    if rule_id == "R14":
        stays = payload.get("stays_or_episodes")
        stay_text = f" Medicare stays in the file: {_count(stays)}." if stays not in (None, "") else ""
        return (
            f"Average Medicare hospice length of stay was {_plain_number(metric)} days, from service days divided by stays. "
            f"The peer median was {_plain_number(peer)} days. The cutoff used here was {_plain_number(threshold)} days."
            + stay_text
            + " The certification number is matched on the last six digits, and on state when both files have a state."
        )
    if rule_id == "R15":
        episodes = payload.get("stays_or_episodes")
        episode_text = f" Medicare episodes in the file: {_count(episodes)}." if episodes not in (None, "") else ""
        return (
            f"Medicare home-health visits per episode were {_plain_number(metric)}. "
            f"The peer median was {_plain_number(peer)}. The cutoff used here was {_plain_number(threshold)}."
            + episode_text
            + " The certification number is matched on the last six digits, and on state when both files have a state."
        )
    if rule_id == "R16":
        return (
            f"Servicing NPI {payload.get('servicing_npi') or 'unknown'} is on the exclusion list "
            f"as of {payload.get('excl_date') or 'an unknown date'}. "
            "The billing NPI is a different number and is not itself on the exclusion list. "
            f"Paid on or after that date in this extract: {_usd(payload.get('paid_on_or_after_exclusion'))}."
        )
    if rule_id == "R17":
        return (
            f"Servicing NPI {payload.get('servicing_npi') or 'unknown'} was paid through "
            f"{_count(payload.get('billers') if payload.get('billers') is not None else metric)} billing NPIs "
            "that do not share an owner. "
            f"The cutoff is {_count(threshold)} billing NPIs. "
            f"Paid through this biller: {_usd(payload.get('paid'))}."
            + _other_billers(payload)
            + " Sharing a clinician is not, by itself, a finding that a claim was false."
        )
    if rule_id in {"R18", "R19"}:
        measure = payload.get("measure") or _TITLE.get(rule_id, rule_id)
        peer_text = f" The peer median was {_plain_number(peer)}." if peer is not None else ""
        cutoff = f" The cutoff used here was {_plain_number(threshold)}." if threshold is not None else ""
        return (
            f"Care Compare measure \"{measure}\" was {_plain_number(metric)}."
            + peer_text
            + cutoff
            + " This is corroboration from Care Compare. It is not a Critical tier by itself, and it does not use the Medicaid claim lines."
        )
    blurb = RULE_BLURB.get(rule_id, "An indicator fired.")
    metric_text = f"{float(metric):.4g}" if metric is not None else "n/a"
    threshold_text = f"{float(threshold):.4g}" if threshold is not None else "n/a"
    peer_text = f" Peer value {float(peer):.4g}." if peer is not None else ""
    gap = f" Peer-gap estimate {_usd(dollars)}." if dollars else ""
    return f"{blurb} Observed {metric_text}. Cutoff {threshold_text}.{peer_text}{gap}"


def _other_billers(payload: dict) -> str:
    others = payload.get("other_billers") or []
    bits = []
    for item in others:
        if not isinstance(item, dict):
            continue
        label = (item.get("name") or "").strip() or "name not in the directory"
        npi = item.get("npi") or "unknown"
        paid = item.get("paid")
        bit = f"{label} (NPI {npi}"
        if paid not in (None, ""):
            bit += f", paid {_usd(paid)}"
        bits.append(bit + ")")
    if not bits:
        return ""
    sentence = (
        " The other billing NPIs, which do not share an owner with this biller, are "
        + "; ".join(bits)
        + "."
    )
    try:
        omitted = int(payload.get("other_billers_omitted") or 0)
    except (TypeError, ValueError):
        omitted = 0
    if omitted > 0:
        sentence += f" {omitted} more are not listed."
    return sentence


def _weight_sentence(settings: Settings, rule_ids: list[str], score_value: float) -> str:
    if not rule_ids:
        return "No indicator weights were stored. Peer-gap dollars are not part of the score."
    weights = []
    total = 0.0
    for rule_id in rule_ids:
        weight = float((settings.rules.get(rule_id) or {}).get("weight", 1))
        total += weight
        weights.append(f"{rule_id} is {weight:g}")
    extras = []
    if len(rule_ids) >= settings.multi_flag_min:
        total += float(settings.multi_flag_bonus)
        extras.append(
            f"{settings.multi_flag_bonus:g} because {len(rule_ids)} indicators fired "
            f"(the add-on starts at {settings.multi_flag_min})"
        )
    if "R11" in rule_ids:
        total += float(R11_BONUS)
        extras.append(f"{R11_BONUS:g} because the billing NPI is on the exclusion list")
    remainder = round(float(score_value) - total, 2)
    if abs(remainder - float(settings.cluster_bonus)) < 0.05:
        total += float(settings.cluster_bonus)
        extras.append(
            f"{settings.cluster_bonus:g} because another flagged provider shares a group "
            f"of at most {settings.cluster_bonus_max_providers} providers"
        )
    elif abs(remainder) >= 0.05:
        total += remainder
        extras.append(f"{remainder:g} more from the stored score")
    sentence = "The score adds a weight for each indicator: " + ", ".join(weights) + "."
    if extras:
        sentence += " It also adds " + "; ".join(extras) + "."
    sentence += f" Those pieces sum to {total:.1f}. Peer-gap dollars are not part of the score."
    return sentence


def _owner_lines(edges: list) -> list[str]:
    grouped: dict[str, dict] = {}
    order: list[str] = []
    for owner_key, owner_name, role, percent, _weight, _origin in edges:
        key = owner_key or "unknown"
        if key not in grouped:
            order.append(key)
            grouped[key] = {"name": (owner_name or "").strip(), "roles": []}
        shown = "CMS stored no percent on this row" if percent is None else f"{float(percent):g}%"
        grouped[key]["roles"].append(f"{role_label(role)} at {shown}")
    lines = []
    for key in order:
        slot = grouped[key]
        who = slot["name"] or "Unnamed owner"
        lines.append(f"{who} (PECOS associate id {key}): " + "; ".join(slot["roles"]) + ".")
    return lines


def _write_pdf(path: Path, blocks: list[tuple[str, str]], drawing) -> None:
    from reportlab.lib.colors import HexColor, white
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.platypus import HRFlowable, KeepTogether, Paragraph, SimpleDocTemplate, Spacer

    navy = HexColor("#1B3A5C")
    ink = HexColor("#142033")
    base = getSampleStyleSheet()
    title = ParagraphStyle(
        "PacketTitle", parent=base["Heading1"], fontName="Helvetica-Bold",
        fontSize=16, leading=20, textColor=navy, spaceAfter=2,
    )
    heading = ParagraphStyle(
        "PacketHead", parent=base["Heading2"], fontName="Helvetica-Bold",
        fontSize=12, leading=15, textColor=navy, spaceBefore=12, spaceAfter=3,
    )
    body = ParagraphStyle(
        "PacketBody", parent=base["BodyText"], fontName="Helvetica",
        fontSize=10, leading=14, textColor=ink, spaceAfter=6,
    )
    small = ParagraphStyle(
        "PacketSmall", parent=body, fontSize=8, leading=11, textColor=HexColor("#5c6b7a"),
    )

    def paint(canvas, doc) -> None:
        canvas.saveState()
        canvas.setFillColor(navy)
        canvas.rect(0, letter[1] - 26, letter[0], 26, fill=1, stroke=0)
        canvas.setFillColor(white)
        canvas.setFont("Helvetica", 8)
        canvas.drawString(48, letter[1] - 16, "Fraud Ledger    Review lead from public data    Not a finding    Not filed")
        canvas.setFillColor(HexColor("#5c6b7a"))
        canvas.setFont("Helvetica", 8)
        canvas.drawString(48, 28, "Peer-gap dollars are not a loss amount and not money taken.")
        canvas.drawRightString(letter[0] - 48, 28, f"Page {doc.page}")
        canvas.restoreState()

    def flow(kind: str, text: str):
        safe = _esc(text)
        if kind == "title":
            return Paragraph(safe, title)
        if kind == "heading":
            return Paragraph(safe, heading)
        if kind == "small":
            return Paragraph(safe, small)
        return Paragraph(safe, body)

    story = []
    index = 0
    while index < len(blocks):
        kind, text = blocks[index]
        if kind == "title":
            story.append(flow(kind, text))
            story.append(HRFlowable(width="100%", thickness=1, color=navy, spaceBefore=2, spaceAfter=8))
            index += 1
            continue
        if kind == "chart":
            if drawing is not None:
                story.append(Spacer(1, 4))
                story.append(drawing)
                story.append(Spacer(1, 2))
            index += 1
            continue
        if kind == "heading" and index + 1 < len(blocks) and blocks[index + 1][0] == "chart":
            grouped = [flow(kind, text)]
            index += 2
            if drawing is not None:
                grouped.extend((Spacer(1, 4), drawing, Spacer(1, 2)))
            if index < len(blocks) and blocks[index][0] == "body":
                grouped.append(flow("body", blocks[index][1]))
                index += 1
            story.append(KeepTogether(grouped))
            continue
        if kind == "heading":
            grouped = [flow(kind, text)]
            index += 1
            while index < len(blocks) and blocks[index][0] in {"body", "small"}:
                grouped.append(flow(blocks[index][0], blocks[index][1]))
                index += 1
                if len(grouped) >= 3:
                    break
            story.append(KeepTogether(grouped))
            continue
        story.append(flow(kind, text))
        index += 1
    doc = SimpleDocTemplate(
        str(path),
        pagesize=letter,
        title="Fraud Ledger review lead",
        leftMargin=48,
        rightMargin=48,
        topMargin=42,
        bottomMargin=44,
    )
    doc.build(story, onFirstPage=paint, onLaterPages=paint)


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
        SELECT name, entity_type, taxonomy, address, city, state, zip,
               enumeration_date, incorporation_date, origin
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
    monthly = con.execute(
        """
        SELECT month, SUM(paid), string_agg(DISTINCT upper(trim(hcpcs)), ',')
        FROM facts_medicaid
        WHERE billing_npi = ?
        GROUP BY 1
        ORDER BY 1
        """,
        [provider_key],
    ).fetchall()
    chart = chart_for(con, settings, provider_key, identity[5] if identity else None, monthly)

    name = (identity[0] if identity and identity[0] else "") or provider_key
    place = ""
    if identity:
        city_state = ", ".join(part for part in (identity[4], identity[5]) if part)
        tail = " ".join(part for part in (city_state, _zip(identity[6])) if part)
        place = ", ".join(part for part in (identity[3], tail) if part)
    paid_total = float(span[2] or 0) if span else 0
    gap_total = unduplicated_excess(
        (rule_id, dollars) for rule_id, _metric, _threshold, _peer, dollars, _detail in flags
    )
    rule_ids = []
    if score and score[2]:
        rule_ids = [part for part in str(score[2]).split(",") if part]
    clauses = [_CLAUSE[rule_id] for rule_id in rule_ids if rule_id in _CLAUSE]
    if clauses:
        indicates = "The data indicates that " + "; ".join(clauses) + "."
    elif flags:
        indicates = "The data indicates the indicators listed below. None of them is a finding that a crime occurred."
    else:
        indicates = "No indicator fired for this provider in this run."
    if gap_total:
        dollars_line = (
            f"Paid in the loaded Medicaid extract: {_usd(paid_total)}. "
            f"Dollars above a typical peer: {_usd(gap_total)}. "
            "A payment jump is counted once. An owner associated with that jump does not add those dollars again. "
            "It is not a loss amount and not money taken."
        )
    else:
        dollars_line = (
            f"Paid in the loaded Medicaid extract: {_usd(paid_total)}. "
            "Dollars above a typical peer: none. These indicators do not carry a dollar gap."
        )

    blocks: list[tuple[str, str]] = [
        ("title", f"{name}"),
        ("small", f"NPI {provider_key}  |  run {run_id}"),
        ("body", "Review lead from public data. Not a finding that a crime occurred. This file was not sent to any agency."),
    ]
    if identity:
        kind = identity[1] or "not loaded"
        if kind.isalpha() and kind == kind.lower():
            kind = kind[:1].upper() + kind[1:]
        formed = identity[8]
        enumerated = identity[7]
        blocks.append((
            "body",
            f"NPPES entity type: {kind}."
            + (f" NPPES taxonomy {identity[2]}. That is the specialty code, not a finding." if identity[2] else "")
            + (f" Directory address: {place}." if place else " No street address was loaded for this provider.")
            + (
                f" NPPES enumeration date: {enumerated}. That is the date the NPI was issued, not an incorporation date."
                if enumerated
                else " NPPES enumeration date: not loaded."
            )
            + (
                f" Incorporation date: {formed}. That date is from the enrollment file. It is not a Medicare enrollment date."
                if formed
                else " Incorporation date: not loaded."
            ),
        ))
    blocks.append(("heading", "Bottom line up front"))
    if score:
        count = int(score[4])
        blocks.append((
            "body",
            f"Review tier: {score[1]}. Score: {float(score[0]):.1f}. "
            f"{count} indicator{'s' if count != 1 else ''}: {score[2] or 'none'}.",
        ))
        blocks.append(("body", indicates))
        blocks.append(("body", dollars_line))
        blocks.append(("body", _weight_sentence(settings, rule_ids, float(score[0]))))
        blocks.append((
            "body",
            "The score orders the review queue. It is not a probability that a crime occurred. "
            f"Critical means the billing NPI is on the exclusion list, or the score is at least {settings.tier_critical:g}. "
            f"High means at least {settings.tier_high:g}. Elevated means at least {settings.tier_elevated:g}. "
            "A lower score is Monitor.",
        ))
    else:
        blocks.append(("body", "No review score was stored for this provider in this run."))
        blocks.append(("body", indicates))
        blocks.append(("body", dollars_line))
    blocks.append(("heading", "Dollar figures"))
    if span and span[4]:
        blocks.append(("body", f"Paid in the loaded Medicaid extract: {_usd(paid_total)}."))
        blocks.append(("body", f"Months in the extract: {_month(span[0])} through {_month(span[1])}."))
        blocks.append((
            "body",
            f"Billing rows kept: {int(span[4]):,}. Claim lines: {float(span[3] or 0):,.0f}.",
        ))
        blocks.append((
            "body",
            "This is the sum of the rows kept for the priority procedure codes. It is not a complete payment history.",
        ))
        if span[4] > len(evidence_rows):
            blocks.append(("body", f"The evidence file has the first {len(evidence_rows)} billing rows, not every row."))
    else:
        blocks.append(("body", "No billing rows for this provider are in the loaded extract."))
    if gap_total:
        blocks.append(("body", f"Dollars above a typical peer: {_usd(gap_total)}."))
        blocks.append((
            "body",
            "That counts a payment jump once. An owner associated with that jump does not add those dollars again. "
            "For a high rate, the estimate on that indicator is (paid per line - peer median) x claim lines. "
            "It is not a loss amount, not money taken, and not a number to type on an intake form as an amount taken.",
        ))
    else:
        blocks.append(("body", "Dollars above a typical peer: none. These indicators do not carry a dollar peer-gap."))
    blocks.append(("heading", "Paid by month"))
    blocks.append(("chart", ""))
    blocks.append(("body", chart["note"]))
    blocks.append(("heading", "What the data indicates"))
    blocks.append((
        "body",
        "Each section below is one indicator. An indicator is a comparison with a peer or a public list. "
        "It is not a finding that a crime occurred. Several indicators can fire at once. "
        "A cost comparison uses other billers in the same state when that state has at least "
        f"{settings.min_peer_n} billers of the procedure code. Otherwise it uses the national median. "
        "A provider that bills only the expected codes for hospice, home health, or personal care "
        "is not flagged for a narrow or flat pattern. A missing organization date is not treated as new. "
        "Care Compare live discharges and readmissions support a review. They are not a Critical tier by themselves.",
    ))
    if not flags:
        blocks.append(("body", "No indicator rows were stored for this provider in this run."))
    for rule_id, metric, threshold, peer, dollars, detail in flags:
        blocks.append(("heading", _heading_for(rule_id, detail)))
        blocks.append(("body", _explain_flag(rule_id, metric, threshold, peer, dollars, detail)))
    blocks.append(("heading", "Ownership"))
    link_rows = [
        (owner_key, owner_name, role, percent)
        for owner_key, owner_name, role, percent, _weight, _origin in edges
    ]
    for line in explain_membership(
        con,
        run_id,
        provider_key,
        identity[3] if identity else None,
        identity[4] if identity else None,
        identity[5] if identity else None,
        link_rows,
        settings.address_link_max,
    ):
        blocks.append(("body", line))
    blocks.append((
        "body",
        "Each line below is one person or company on this provider. "
        "The id in parentheses is the PECOS associate id, column ASSOCIATE ID - OWNER in the CMS owner file. It is not an NPI. "
        "The words after the colon are the relationship CMS recorded. "
        "Direct owner (CMS role 34) means equity in this provider. Count each associate once. "
        "Those direct-owner percents should generally come to about 100 percent. "
        "Indirect owner (CMS role 35) means equity in a company that owns this provider. Do not add that percent on top of the direct owner. "
        "Officer (CMS role 40), director (CMS role 41), and managing employee are control relationships. "
        "Medicare requires those even when the same person is already listed as an owner, and the percent on those rows is usually the same stake again. "
        "CMS stored no percent means the percent cell was blank. It does not mean zero. "
        "Several relationships for the same id are one owner, not several shares.",
    ))
    owner_lines = _owner_lines(edges)
    if not owner_lines:
        blocks.append(("body", "No ownership rows were kept for this provider."))
    else:
        shown = owner_lines[:40]
        blocks.extend(("body", line) for line in shown)
        if len(owner_lines) > len(shown):
            blocks.append(("body", f"{len(owner_lines) - len(shown)} more owners are in the database and are not listed here."))
    blocks.append(("heading", "Exclusion list"))
    if not leie:
        blocks.append(("body", "No exclusion-list row was loaded for this billing NPI."))
    for excluded_name, excl_type, excl_date, origin in leie:
        blocks.append((
            "body",
            f"Name on the list: {excluded_name or 'blank'}. Type {excl_type or 'blank'}. "
            f"Date {excl_date or 'blank'}. Source {origin}.",
        ))
    blocks.append(("heading", "Where the figures come from"))
    blocks.append((
        "small",
        f"Config SHA-256 {settings.config_sha256}. The thresholds travel with the run.",
    ))
    if not snapshots:
        blocks.append(("body", "No download snapshot was stored. The extract may have been loaded from a local path."))
    for source_id, url, retrieved, digest, nbytes in snapshots:
        blocks.append(("small", f"{source_id} retrieved {retrieved}. {nbytes} bytes. SHA-256 {digest}. {url}"))
    blocks.append(("body", DISCLAIMER))
    blocks.append(("heading", "How to file this"))
    blocks.append(("body", f"Open {INTAKE} and file it yourself if you decide it is worth sending. This program does not file it and does not compute a reward."))
    blocks.append(("body", "Attach report.pdf. If the form asks for an amount, do not type the peer-gap estimate as money taken. Leave the amount blank or say the attachment shows a gap versus a peer median."))
    blocks.append(("body", "The Criminal Tip Rewards Pilot is discretionary."))

    narrative = []
    for kind, text in blocks:
        if kind == "chart":
            continue
        if kind in {"title", "heading"}:
            narrative.append("")
        narrative.append(text)
    narrative_text = "\n".join(narrative).strip() + "\n"
    _reject(narrative_text)

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
    drawing = None
    if chart["points"]:
        drawing = paid_drawing(chart["points"], chart["benchmark"], chart["peer_label"])
    _write_pdf(pdf_path, blocks, drawing)
    dest = dest_dir / f"{run_id}-{provider_key}.zip"
    with zipfile.ZipFile(dest, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("narrative.txt", narrative_text)
        archive.writestr("evidence.csv", evidence_csv)
        archive.writestr("indicators.csv", indicator_csv)
        archive.writestr("how-to-submit.txt", submit)
        archive.write(pdf_path, "report.pdf")
    pdf_path.unlink(missing_ok=True)
    return dest
