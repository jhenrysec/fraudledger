"""Procedure codes the public working set keeps from the Medicaid extract."""

from __future__ import annotations

import re

# Hospice, home health, and personal-care HCPCS. The national extract is still
# downloaded in full; only these codes are written into the warehouse.
_HOSPICE = (
    "T2042",
    "T2043",
    "T2044",
    "T2045",
    "T2046",
    "Q5001",
    "Q5002",
    "Q5003",
    "Q5004",
    "Q5005",
    "Q5006",
    "Q5007",
    "Q5008",
    "Q5009",
    "Q5010",
    "G0337",
    "S9126",
)
_HOME_HEALTH = (
    "G0151",
    "G0152",
    "G0153",
    "G0154",
    "G0155",
    "G0156",
    "G0157",
    "G0158",
    "G0159",
    "G0160",
    "G0161",
    "G0162",
    "G0299",
    "G0300",
    "G0493",
    "G0494",
    "G0495",
    "G0496",
    "S9122",
    "S9123",
    "S9124",
    "T1021",
    "T1022",
)
_PERSONAL_CARE = (
    "T1019",
    "T1020",
    "S5125",
    "S5126",
    "S5130",
    "S5131",
)

PRIORITY_HCPCS = frozenset(_HOSPICE + _HOME_HEALTH + _PERSONAL_CARE)
HOSPICE_HCPCS = frozenset(_HOSPICE)


def priority_in_list() -> str:
    """SQL string list. Codes are a fixed set, not operator input."""
    quoted = []
    for code in sorted(PRIORITY_HCPCS):
        if not re.fullmatch(r"[A-Z0-9]{5}", code):
            raise RuntimeError(f"Priority procedure code is not 5 characters: {code}")
        quoted.append("'" + code + "'")
    return ", ".join(quoted)
