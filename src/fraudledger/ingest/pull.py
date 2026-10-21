"""Download a known public file, or a URL the operator pasted."""

from __future__ import annotations

import duckdb

from fraudledger.ingest.catalog import resolve_file_url
from fraudledger.ingest.public import ingest_enrollments, ingest_leie, ingest_medicaid, ingest_nppes, ingest_ownership
from fraudledger.settings import Settings

PAIRS = {
    "hospice": ("hospice-enrollments", "hospice-owners"),
    "home-health": ("home-health-enrollments", "home-health-owners"),
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
    if key in PAIRS:
        if url:
            raise SystemExit("Paste a URL with a single file type, not the combined owner source.")
        enrollment_key, owner_key = PAIRS[key]
        enrollment_url = resolve_file_url(settings, enrollment_key)
        owner_url = resolve_file_url(settings, owner_key)
        enrollments = ingest_enrollments(con, settings, url=enrollment_url, kind=enrollment_key)
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
    parts.append(pull(con, settings, "medicaid", drop_demo=True, focus="priority"))
    return " ".join(parts)
