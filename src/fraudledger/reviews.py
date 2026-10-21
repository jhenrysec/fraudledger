from __future__ import annotations

import re
from datetime import datetime, timezone

import duckdb

ALLOWED_STATES = ("unreviewed", "watch", "dismissed", "exported")
PROVIDER_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$")


def set_review(con: duckdb.DuckDBPyConnection, provider_key: str, state: str, note: str = "") -> None:
    if not PROVIDER_KEY.match(provider_key or ""):
        raise SystemExit(f"Provider key is not a safe identifier: {provider_key!r}")
    if state not in ALLOWED_STATES:
        raise SystemExit(f"Review state must be one of: {', '.join(ALLOWED_STATES)}")
    cleaned = (note or "").strip()
    if len(cleaned) > 2000:
        raise SystemExit("Review note is limited to 2000 characters.")
    con.execute(
        """
        INSERT INTO review_states (provider_key, state, note, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (provider_key) DO UPDATE SET
            state = excluded.state,
            note = excluded.note,
            updated_at = excluded.updated_at
        """,
        [provider_key, state, cleaned, datetime.now(timezone.utc).replace(tzinfo=None)],
    )
