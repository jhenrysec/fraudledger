"""Reader-facing definitions. The page and the report packet use the same numbers."""

from __future__ import annotations

from fraudledger.detect.engine import CENSUS_SHARE, HOSPICE_TAXONOMY, RULE_BLURB
from fraudledger.detect.score import R11_BONUS
from fraudledger.settings import Settings

# CMS PECOS owner role codes. Text is the Owner Role Code Reference Table in the
# Hospital All Owners data dictionary (CMS, July 2026). Home health and hospice
# owner files use the same codes.
ROLE_CODES = (
    ("01", "5% or more ownership interest", "Older combined ownership code. Do not add it on top of a role 34 row for the same owner."),
    ("03", "Partner", "Control role. Kept even under the percent floor. Not another slice of the direct total."),
    ("25", "Contracted managing employee", "Control role. Kept even under the percent floor. The percent is not equity to add."),
    ("34", "5% or greater direct ownership interest", "Direct equity in this provider. Sum once per owner. That sum should generally be about 100%."),
    ("35", "5% or greater indirect ownership interest", "Equity in an owner of this provider. Keep it as its own layer. Do not add it to role 34."),
    ("36", "5% or greater mortgage interest", "A financial interest, not equity. Do not add it to the direct total."),
    ("37", "5% or greater security interest", "A financial interest, not equity. Do not add it to the direct total."),
    ("38", "General partnership interest", "Partnership interest. Do not add it to role 34."),
    ("39", "Limited partnership interest", "Partnership interest. Do not add it to the direct total."),
    ("40", "Officer", "Control role. Often repeats an ownership percent already listed. Do not add it again."),
    ("41", "Director", "Control role. Often repeats an ownership percent already listed. Do not add it again."),
    ("42", "W-2 managing employee", "Control role. Kept even under the percent floor. Do not add it to the equity total."),
    ("43", "Operational/managerial control", "Control role. Kept even under the percent floor. Do not add it to the equity total."),
    ("44", "Other", "Kept only when the percent meets the floor. Read the source role text before you total it."),
)

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
                f" + {index['cluster_bonus']:.1f} when another flagged provider shares the cluster."
            ),
            (
                f"Critical means R11 fired or the score is at least {index['tier_critical']:g}. "
                f"High is at least {index['tier_high']:g}. "
                f"Elevated is at least {index['tier_elevated']:g}. "
                "Any other flagged provider is Monitor."
            ),
            "An ownership edge is one role for one owner. Do not add the percent column.",
            "Add role 34 once per owner for the direct share. CMS says that direct total should generally be about 100%.",
            "Role 35 is indirect ownership of an owner. Do not add it to role 34.",
            "Roles 40 and 41 are officer and director. Role 42 is a W-2 managing employee. The same percent on those rows is not a second stake.",
            "Weight = role strength x percent / 100. It groups providers. It is not a share.",
            "The workstation How to read page has the full role-code index and the federal sources.",
        ]
    )
