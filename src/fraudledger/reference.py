"""Reader-facing definitions. The page and the report packet use the same numbers."""

from __future__ import annotations

import math

from fraudledger.detect.engine import CENSUS_SHARE, HOSPICE_TAXONOMY, RATE_SHARE, RULE_BLURB
from fraudledger.detect.score import R11_BONUS
from fraudledger.settings import Settings

# CMS PECOS owner role codes. Text is the Owner Role Code Reference Table in the
# Hospital All Owners data dictionary (CMS, July 2026). Home health and hospice
# owner files use the same codes.
ROLE_CODES = (
    ("01", "5% or more ownership interest", "Older combined ownership code. Do not add it on top of a direct-owner row (CMS role 34) for the same owner."),
    ("03", "Partner", "Control role. Kept even under the percent floor. Not another slice of the direct total."),
    ("25", "Contracted managing employee", "Control role. Kept even under the percent floor. The percent is not equity to add."),
    ("34", "5% or greater direct ownership interest", "Direct equity in this provider. Sum once per owner. That sum should generally be about 100%."),
    ("35", "5% or greater indirect ownership interest", "Equity in a company that owns this provider. Keep it as its own layer. Do not add it to the direct-owner percent (CMS role 34)."),
    ("36", "5% or greater mortgage interest", "A financial interest, not equity. Do not add it to the direct total."),
    ("37", "5% or greater security interest", "A financial interest, not equity. Do not add it to the direct total."),
    ("38", "General partnership interest", "Partnership interest. Do not add it to the direct-owner percent (CMS role 34)."),
    ("39", "Limited partnership interest", "Partnership interest. Do not add it to the direct total."),
    ("40", "Officer", "Control role. Often repeats an ownership percent already listed. Do not add it again."),
    ("41", "Director", "Control role. Often repeats an ownership percent already listed. Do not add it again."),
    ("42", "W-2 managing employee", "Control role. Kept even under the percent floor. Do not add it to the equity total."),
    ("43", "Operational/managerial control", "Control role. Kept even under the percent floor. Do not add it to the equity total."),
    ("44", "Other", "Kept only when the percent meets the floor. Read the source role text before you total it."),
)

_SHORT = {
    "01": "owner, older combined code",
    "03": "partner",
    "25": "contracted managing employee",
    "34": "direct owner",
    "35": "indirect owner",
    "36": "mortgage interest",
    "37": "security interest",
    "38": "general partner",
    "39": "limited partner",
    "40": "officer",
    "41": "director",
    "42": "W-2 managing employee",
    "43": "operational control",
    "44": "other relationship",
}

_ORIGIN = {
    "public:home-health-owners": "CMS home-health owner file",
    "public:hospice-owners": "CMS hospice owner file",
    "public:home-health-enrollments": "CMS home-health enrollment file",
    "public:hospice-enrollments": "CMS hospice enrollment file",
    "public:nppes": "NPPES monthly file",
    "public:leie": "OIG exclusion list",
    "public:medicaid": "Medicaid Provider Spending",
    "public": "a public file",
}


def role_key(code: object) -> str:
    raw = "" if code is None else str(code).strip()
    if raw.endswith(".0") and raw[:-2].replace(".", "", 1).isdigit():
        raw = raw[:-2]
    if not raw:
        return ""
    if raw.isdigit():
        trimmed = raw.lstrip("0") or "0"
        if len(trimmed) == 1:
            return trimmed.zfill(2)
        return trimmed
    return raw


def role_label(code: object) -> str:
    """The relationship CMS recorded, with the role code beside it.

    A bare number such as 34 is direct ownership of this provider. 35 is
    ownership of a company that owns this provider. 40 and 41 are officer
    and director. Those control rows are not a second share.
    """
    key = role_key(code)
    if not key:
        return "relationship not recorded"
    short = _SHORT.get(key)
    if short:
        return f"{short} (CMS role {key})"
    shown = "" if code is None else str(code).strip()
    return f"CMS role {shown or key} (not in the CMS role table used here)"


def origin_label(origin: object) -> str:
    text = "" if origin is None else str(origin).strip()
    if text in _ORIGIN:
        return _ORIGIN[text]
    if text.startswith("public:"):
        rest = text.split(":", 1)[1].replace("-", " ")
        return f"public file: {rest}"
    if not text:
        return "source not recorded"
    return text

_MONEY = {"min_paid", "min_abs_change", "min_latest_paid", "min_first_year_paid"}
_LABELS = {
    "multiple_of_median": "multiple of the peer median",
    "min_paid": "minimum paid",
    "min_codes": "minimum codes above the 90th percentile",
    "max_lines_per_beneficiary": "maximum lines per beneficiary",
    "lines_per_working_day": "lines per working day",
    "working_days_per_month": "working days assumed in a month",
    "max_codes": "maximum distinct codes",
    "max_cv": "maximum coefficient of variation",
    "min_months": "minimum months",
    "min_yoy": "minimum year-over-year ratio",
    "min_abs_change": "minimum dollar change",
    "min_latest_paid": "minimum paid in the latest year",
    "min_first_year_paid": "minimum first-year paid",
    "enumeration_on_or_after": "organization date on or after",
    "min_stays": "minimum stays",
    "min_episodes": "minimum episodes",
    "min_providers": "minimum peer providers",
    "min_billers": "minimum billing NPIs",
}

_EXAMPLES = (("34", 100.0), ("40", 51.0), ("35", 51.0), ("42", 2.0))


def _cutoff(cfg: dict) -> str:
    parts = []
    for key, value in cfg.items():
        if key in {"title", "weight"}:
            continue
        label = _LABELS.get(key, key.replace("_", " "))
        if key in _MONEY and isinstance(value, (int, float)):
            shown = f"${value:,.0f}"
        else:
            shown = str(value)
        parts.append(f"{label} {shown}")
    return "; ".join(parts) if parts else "No extra numeric cutoff."


def reader_index(settings: Settings) -> dict:
    floor = float(settings.min_ownership_pct)
    control = {str(code) for code in settings.control_roles}
    roles = []
    for code, cms, how in ROLE_CODES:
        strength = float(settings.role_strength.get(code, settings.default_role_strength))
        roles.append(
            {
                "code": code,
                "cms": cms,
                "how": how,
                "strength": f"{strength:.1f}",
                "kept": (
                    f"Yes, even under {floor:g}%"
                    if code in control
                    else f"Only at {floor:g}% or more"
                ),
            }
        )
    examples = []
    for code, percent in _EXAMPLES:
        strength = float(settings.role_strength.get(code, settings.default_role_strength))
        examples.append(
            {
                "code": code,
                "name": role_label(code),
                "percent": f"{percent:g}",
                "strength": f"{strength:.1f}",
                "weight": f"{strength * percent / 100:.2f}",
            }
        )
    rules = []
    for rule_id, blurb in RULE_BLURB.items():
        cfg = dict(settings.rules.get(rule_id) or {})
        rules.append(
            {
                "id": rule_id,
                "title": cfg.get("title") or rule_id,
                "weight": cfg.get("weight", ""),
                "meaning": blurb,
                "cutoff": _cutoff(cfg),
            }
        )
    return {
        "min_ownership_pct": floor,
        "min_peer_n": settings.min_peer_n,
        "multi_flag_bonus": float(settings.multi_flag_bonus),
        "multi_flag_min": int(settings.multi_flag_min),
        "cluster_bonus": float(settings.cluster_bonus),
        "tier_critical": float(settings.tier_critical),
        "tier_high": float(settings.tier_high),
        "tier_elevated": float(settings.tier_elevated),
        "r11_extra": float(R11_BONUS),
        "census_share": float(CENSUS_SHARE),
        "rate_share": float(RATE_SHARE),
        "cluster_bonus_max": int(settings.cluster_bonus_max_providers),
        "address_link_max": int(settings.address_link_max),
        "default_role_strength": float(settings.default_role_strength),
        "hospice_taxonomy": ", ".join(HOSPICE_TAXONOMY),
        "roles": roles,
        "examples": examples,
        "rules": rules,
        "config_sha": settings.config_sha256,
    }


def reading_notes(settings: Settings) -> str:
    index = reader_index(settings)
    return "\n".join(
        [
            "How to read the numbers",
            "The score orders review. It is not a probability and not a dollar amount.",
            (
                "Score = sum of distinct indicator weights"
                f" + {index['multi_flag_bonus']:.1f} when at least {index['multi_flag_min']} indicators fired"
                f" + {index['r11_extra']:.1f} when R11 fired"
                f" + {index['cluster_bonus']:.1f} when another flagged provider shares a group of at most {index['cluster_bonus_max']} providers."
            ),
            (
                f"Critical means R11 fired or the score is at least {index['tier_critical']:g}. "
                f"High is at least {index['tier_high']:g}. "
                f"Elevated is at least {index['tier_elevated']:g}. "
                "Any other flagged provider is Monitor."
            ),
            "A cost peer is the provider's state when that state has enough billers of the code. Otherwise it is national.",
            "An ownership line is one person or company, and one relationship CMS recorded.",
            "Direct owner (CMS role 34) is equity in this provider. Count that percent once per owner. CMS says that direct total should generally be about 100%.",
            "Indirect owner (CMS role 35) is equity in a company that owns this provider. Do not add it to the direct-owner percent.",
            "Officer (CMS role 40), director (CMS role 41), and W-2 managing employee (CMS role 42) are control relationships. The same percent on those rows is not a second stake.",
            "Weight = role strength x percent / 100. It groups providers. It is not a share.",
            "The workstation How to read page has the full role-code index and the federal sources.",
        ]
    )


def _points(value: float) -> str:
    if abs(value - round(value)) < 0.05:
        return str(int(round(value)))
    return f"{value:.1f}"


def explain_cluster(
    providers: int,
    owners: int,
    flagged: int,
    score: float,
    excess: float | None,
) -> dict:
    """Plain-language reading of one ownership group.

    The stored score is a sort key. It is not the provider score, where 8 is Critical.
    """
    providers = int(providers or 0)
    owners = int(owners or 0)
    flagged = int(flagged or 0)
    score = float(score or 0)
    dollars = float(excess or 0)
    quiet = max(providers - flagged, 0)
    flag_points = flagged * 2.0
    dollar_points = math.log1p(max(dollars, 0.0))
    size_points = min(providers, 10) * 0.5
    has_exclusion = (score - flag_points - dollar_points - size_points) > 1.5
    if flagged == 0:
        headline = f"No billing indicator among {providers} linked providers"
    else:
        share = f"{flagged / providers:.0%}" if providers else "0%"
        headline = f"{flagged} of {providers} providers ({share}) have a billing indicator"
    if quiet == 0:
        quiet_line = "Every provider in this group has a billing indicator."
    elif quiet == 1:
        quiet_line = "The other provider is listed because of a shared owner or a shared street address. It has no billing indicator in this run."
    else:
        quiet_line = (
            f"The other {quiet} are listed because they share an owner or a street address. "
            "They have no billing indicator in this run."
        )
    if owners == 0:
        link = "Same street address"
    else:
        link = "Shared owners"
    pieces = [
        f"{_points(flag_points)} for the {flagged} {'provider' if flagged == 1 else 'providers'} with an indicator",
        f"{_points(dollar_points)} for the size of the peer-gap total",
        f"{_points(size_points)} for group size, which stops counting after 10 providers",
    ]
    if has_exclusion:
        pieces.append("3 because a billing NPI in the group is on the exclusion list")
    sort_sentence = (
        f"The sort key is {_points(score)}. It is not a severity score, and it is not the provider score "
        f"(8 or more is Critical on a provider). It is {', '.join(pieces)}."
    )
    if dollars:
        gap_sentence = (
            f"Added peer-gap estimates: ${dollars:,.0f}. "
            "Each figure is a gap versus a peer median. The peer group is the provider's state when that state has enough billers of the code. Otherwise it is national. "
            "The usual formula is (rate - median) x claim lines. "
            "Adding them is not the amount paid, and it is not a loss amount. "
            "A payment jump is counted once. An owner associated with that jump does not add those dollars again."
        )
    else:
        gap_sentence = "The indicators in this group do not carry a dollar peer gap."
    return {
        "headline": headline,
        "quiet_line": quiet_line,
        "link": link,
        "sort_sentence": sort_sentence,
        "gap_sentence": gap_sentence,
        "providers": providers,
        "owners": owners,
        "flagged": flagged,
        "quiet": quiet,
        "dollars": dollars,
        "sort_key": score,
        "has_exclusion": has_exclusion,
    }
