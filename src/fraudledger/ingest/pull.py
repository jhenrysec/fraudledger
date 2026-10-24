"""Download a known public file, or a URL the operator pasted."""

from __future__ import annotations

import duckdb

from fraudledger.ingest.catalog import resolve_file_url
from fraudledger.ingest.common import report_progress, set_progress_label
from fraudledger.ingest.public import (
    ingest_enrollments,
    ingest_leie,
    ingest_medicaid,
    ingest_medicare_use,
    ingest_nppes,
    ingest_ownership,
    ingest_care_compare,
)
from fraudledger.settings import Settings

PAIRS = {
    "hospice": ("hospice-enrollments", "hospice-owners"),
    "home-health": ("home-health-enrollments", "home-health-owners"),
}

OPTIONAL_KEYS = {
    "nppes",
    "medicare-hospice",
    "medicare-home-health",
    "hospice-care",
    "home-health-care",
}

LABELS = {
    "medicaid": "Medicaid Provider Spending",
    "leie": "OIG exclusion list",
    "nppes": "NPPES monthly file",
    "hospice": "Hospice files",
    "home-health": "Home health files",
    "hospice-owners": "Hospice owners",
    "hospice-enrollments": "Hospice enrollments",
    "home-health-owners": "Home health owners",
    "home-health-enrollments": "Home health enrollments",
    "medicare-hospice": "Medicare hospice use",
    "medicare-home-health": "Medicare home health use",
    "hospice-care": "Hospice Care Compare",
    "home-health-care": "Home health Care Compare",
}


def pull(
    con: duckdb.DuckDBPyConnection,
    settings: Settings,
    key: str,
    url: str | None = None,
    drop_demo: bool = False,
    focus: str | None = None,
) -> str:
    """Load one known source, or one custom URL already tagged with a file kind."""
    label = LABELS.get(key, key)
    set_progress_label(label)
    report_progress(label, 0, None)
    if key in PAIRS:
        if url:
            raise SystemExit("Paste a URL with a single file type, not the combined owner source.")
        enrollment_key, owner_key = PAIRS[key]
        enrollment_url = resolve_file_url(settings, enrollment_key)
        owner_url = resolve_file_url(settings, owner_key)
        set_progress_label(LABELS[enrollment_key])
        report_progress(LABELS[enrollment_key], 0, None)
        enrollments = ingest_enrollments(con, settings, url=enrollment_url, kind=enrollment_key)
        set_progress_label(LABELS[owner_key])
        report_progress(LABELS[owner_key], 0, None)
        edges = ingest_ownership(con, settings, url=owner_url, kind=owner_key)
        return (
            f"{key}: {enrollments} enrollment rows from {enrollment_url}; "
            f"{edges} owner edges from {owner_url}"
        )
    if key == "medicaid":
        chosen = resolve_file_url(settings, "medicaid", url)
        count = ingest_medicaid(con, settings, url=chosen, drop_demo=drop_demo, focus=focus)
        kept = ""
        if focus == "priority":
            kept = " Kept hospice, home health, and personal-care procedure codes."
        return f"Medicaid facts files in view: {count}. Source {chosen}.{kept}"
    if key == "leie":
        chosen = resolve_file_url(settings, "leie", url)
        count = ingest_leie(con, settings, url=chosen)
        return f"LEIE NPI rows: {count}. Source {chosen}"
    if key == "nppes":
        chosen = resolve_file_url(settings, "nppes", url)
        count = ingest_nppes(con, settings, url=chosen)
        return f"NPPES NPI rows staged: {count}. Source {chosen}"
    if key in {"hospice-owners", "home-health-owners"}:
        chosen = resolve_file_url(settings, key, url)
        count = ingest_ownership(con, settings, url=chosen, kind=key)
        return f"Ownership edges kept: {count}. Source {chosen}"
    if key in {"hospice-enrollments", "home-health-enrollments"}:
        chosen = resolve_file_url(settings, key, url)
        count = ingest_enrollments(con, settings, url=chosen, kind=key)
        return f"Enrollment rows: {count}. Source {chosen}"
    if key in {"medicare-hospice", "medicare-home-health"}:
        chosen = resolve_file_url(settings, key, url)
        setting = "hospice" if key == "medicare-hospice" else "home-health"
        count = ingest_medicare_use(con, settings, url=chosen, setting=setting)
        return f"Medicare {setting} provider rows: {count}. Source {chosen}"
    if key in {"hospice-care", "home-health-care"}:
        chosen = resolve_file_url(settings, key, url)
        setting = "hospice" if key == "hospice-care" else "home-health"
        count = ingest_care_compare(con, settings, url=chosen, setting=setting)
        return f"Care Compare {setting} rows: {count}. Source {chosen}"
    raise SystemExit(f"Unknown source: {key}")


def plan(settings: Settings, key: str, url: str | None = None, focus: str | None = None, drop_demo: bool = False) -> list:
    """Resolve download URLs. No database connection.

    A combined hospice or home-health key becomes the enrollment file, then the owner file.
    """
    if key in PAIRS:
        if url:
            raise SystemExit("Paste a URL with a single file type, not the combined owner source.")
        enrollment_key, owner_key = PAIRS[key]
        return [
            (enrollment_key, resolve_file_url(settings, enrollment_key), {}),
            (owner_key, resolve_file_url(settings, owner_key), {}),
        ]
    if key == "skip":
        raise SystemExit("Unknown source: skip")
    chosen = resolve_file_url(settings, key, url)
    options: dict = {}
    if key == "medicaid":
        options = {"drop_demo": drop_demo, "focus": focus}
    return [(key, chosen, options)]


def plan_working(settings: Settings) -> list:
    """URLs for the public working set. NPPES is optional when its index is down."""
    steps = []
    steps.extend(plan(settings, "leie"))
    steps.extend(plan(settings, "hospice"))
    steps.extend(plan(settings, "home-health"))
    try:
        steps.extend(plan(settings, "nppes"))
    except SystemExit as exc:
        steps.append(("skip", str(exc), {}))
    for key in ("medicare-hospice", "medicare-home-health", "hospice-care", "home-health-care"):
        try:
            steps.extend(plan(settings, key))
        except SystemExit as exc:
            steps.append(("skip", str(exc), {}))
    steps.extend(plan(settings, "medicaid", focus="priority", drop_demo=True))
    return steps


def load_local(
    con: duckdb.DuckDBPyConnection,
    settings: Settings,
    key: str,
    path,
    url: str,
    drop_demo: bool = False,
    focus: str | None = None,
) -> str:
    """Ingest a file that is already on disk. The caller should not be mid-download."""
    from pathlib import Path

    local = str(Path(path))
    label = LABELS.get(key, key)
    set_progress_label(label)
    report_progress(f"Reading {label}", None, None)
    if key == "medicaid":
        count = ingest_medicaid(con, settings, path=local, url=url, drop_demo=drop_demo, focus=focus)
        kept = " Kept hospice, home health, and personal-care procedure codes." if focus == "priority" else ""
        return f"Medicaid facts files in view: {count}. Source {url}.{kept}"
    if key == "leie":
        count = ingest_leie(con, settings, path=local, url=url)
        return f"LEIE NPI rows: {count}. Source {url}"
    if key == "nppes":
        count = ingest_nppes(con, settings, path=local, url=url)
        return f"NPPES NPI rows staged: {count}. Source {url}"
    if key in {"hospice-owners", "home-health-owners"}:
        count = ingest_ownership(con, settings, path=local, url=url, kind=key)
        return f"Ownership edges kept: {count}. Source {url}"
    if key in {"hospice-enrollments", "home-health-enrollments"}:
        count = ingest_enrollments(con, settings, path=local, url=url, kind=key)
        return f"Enrollment rows: {count}. Source {url}"
    if key in {"medicare-hospice", "medicare-home-health"}:
        setting = "hospice" if key == "medicare-hospice" else "home-health"
        count = ingest_medicare_use(con, settings, path=local, url=url, setting=setting)
        return f"Medicare {setting} provider rows: {count}. Source {url}"
    if key in {"hospice-care", "home-health-care"}:
        setting = "hospice" if key == "hospice-care" else "home-health"
        count = ingest_care_compare(con, settings, path=local, url=url, setting=setting)
        return f"Care Compare {setting} rows: {count}. Source {url}"
    raise SystemExit(f"Unknown source: {key}")


def pull_working(con: duckdb.DuckDBPyConnection, settings: Settings) -> str:
    """LEIE, hospice, home health, optional NPPES, then priority Medicaid codes.

    Demonstration rows are removed when the Medicaid extract loads.
    """
    parts = [
        pull(con, settings, "leie"),
        pull(con, settings, "hospice"),
        pull(con, settings, "home-health"),
    ]
    try:
        parts.append(pull(con, settings, "nppes"))
    except SystemExit as exc:
        parts.append(f"NPPES skipped: {exc}")
    for key in ("medicare-hospice", "medicare-home-health", "hospice-care", "home-health-care"):
        try:
            parts.append(pull(con, settings, key))
        except SystemExit as exc:
            parts.append(f"{key} skipped: {exc}")
    parts.append(pull(con, settings, "medicaid", drop_demo=True, focus="priority"))
    return " ".join(parts)
