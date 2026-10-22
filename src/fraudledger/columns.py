from __future__ import annotations

import re


def normalize_header(name: str) -> str:
    cleaned = name.replace("\ufeff", "").strip().lower()
    return re.sub(r"[^a-z0-9]+", "_", cleaned).strip("_")


def resolve_columns(fieldnames: list[str]) -> dict[str, str]:
    """Map source headers onto canonical names. Raises ValueError if unpaid."""
    lookup = {normalize_header(name): name for name in fieldnames}

    def pick(*candidates: str) -> str | None:
        for candidate in candidates:
            if candidate in lookup:
                return lookup[candidate]
        return None

    billing = pick("billing_provider_npi_num", "billing_npi", "npi")
    hcpcs = pick("hcpcs_code", "hcpcs", "proc_code")
    month = pick("claim_from_month", "month", "claim_month")
    paid = pick("total_paid", "paid", "amount_paid")
    lines = pick("total_claim_lines", "total_claims", "claim_lines", "claims")
    beneficiaries = pick(
        "total_unique_beneficiaries",
        "beneficiaries",
        "unique_beneficiaries",
        "total_patients",
    )
    servicing = pick("servicing_provider_npi_num", "servicing_npi")
    missing = [
        label
        for label, value in (
            ("billing NPI", billing),
            ("HCPCS", hcpcs),
            ("month", month),
            ("paid", paid),
            ("claim lines", lines),
        )
        if value is None
    ]
    if missing:
        raise ValueError(
            "Unrecognized Medicaid spending columns; missing "
            + ", ".join(missing)
            + f". Headers seen: {fieldnames[:24]}"
        )
    return {
        "billing_npi": billing,
        "servicing_npi": servicing or "",
        "hcpcs": hcpcs,
        "month": month,
        "paid": paid,
        "claim_lines": lines,
        "beneficiaries": beneficiaries or "",
    }
