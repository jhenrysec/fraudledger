from __future__ import annotations

from fraudledger.settings import Settings


def qualifies(percentage: float | None, role_code: str | None, settings: Settings) -> bool:
    if percentage is not None and percentage >= settings.min_ownership_pct:
        return True
    return str(role_code or "") in set(settings.control_roles)


def weight(percentage: float | None, role_code: str | None, settings: Settings) -> float:
    strength = settings.role_strength.get(str(role_code or ""), settings.default_role_strength)
    percent = settings.min_ownership_pct if percentage is None else percentage
    return float(strength) * float(percent) / 100.0
