"""Drop leftover local rows that are not a public extract.

Older builds could write a reserved NPI prefix and an origin of demo.
Those rows are not public data. Public extracts are left in place.
"""

from __future__ import annotations

import re
import shutil

import duckdb

from fraudledger.db import refresh_facts_view
from fraudledger.settings import Settings

_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")


def _inside(path, root) -> bool:
    resolved = path.resolve()
    base = root.resolve()
    return resolved != base and base in resolved.parents


def purge_reserved(con: duckdb.DuckDBPyConnection, settings: Settings) -> str:
    """Delete the retired local prefix and any run built only from it."""
    settings.ensure_dirs()
    demo_file = settings.data_dir / "parquet" / "facts" / "demo.parquet"
    keys: set[str] = set()
    if demo_file.is_file():
        keys.update(
            row[0]
            for row in con.execute(
                "SELECT DISTINCT billing_npi FROM read_parquet(?) WHERE origin = 'demo'",
                [str(demo_file)],
            ).fetchall()
            if row[0]
        )
    keys.update(
        row[0]
        for row in con.execute(
            """
            SELECT provider_key FROM providers
            WHERE origin = 'demo' OR provider_key LIKE '199999%'
            """
        ).fetchall()
        if row[0]
    )
    run_ids = [
        row[0]
        for row in con.execute("SELECT run_id FROM runs WHERE demo_only = TRUE").fetchall()
        if row[0] and _RUN_ID.match(row[0])
    ]
    if demo_file.is_file():
        demo_file.unlink()
    refresh_facts_view(con, settings.data_dir / "parquet" / "facts")

    con.execute("DELETE FROM providers WHERE origin = 'demo' OR provider_key LIKE '199999%'")
    con.execute("DELETE FROM leie WHERE origin = 'demo' OR npi LIKE '199999%'")
    con.execute("DELETE FROM ownership_edges WHERE origin = 'demo' OR provider_key LIKE '199999%'")
    con.execute("DELETE FROM enrollments WHERE origin = 'demo' OR npi LIKE '199999%'")
    con.execute("DELETE FROM review_states WHERE provider_key LIKE '199999%'")
    if keys:
        con.executemany("DELETE FROM flags WHERE provider_key = ?", [(key,) for key in keys])
        con.executemany("DELETE FROM scores WHERE provider_key = ?", [(key,) for key in keys])
        con.executemany("DELETE FROM cluster_members WHERE provider_key = ?", [(key,) for key in keys])
    for run_id in run_ids:
        con.execute("DELETE FROM flags WHERE run_id = ?", [run_id])
        con.execute("DELETE FROM scores WHERE run_id = ?", [run_id])
        con.execute("DELETE FROM cluster_members WHERE run_id = ?", [run_id])
        con.execute("DELETE FROM clusters WHERE run_id = ?", [run_id])
        con.execute("DELETE FROM run_artifacts WHERE run_id = ?", [run_id])
        con.execute("DELETE FROM runs WHERE run_id = ?", [run_id])
        run_dir = settings.data_dir / "runs" / run_id
        if run_dir.exists() and _inside(run_dir, settings.data_dir / "runs"):
            shutil.rmtree(run_dir)
        export = settings.data_dir / "exports" / f"{run_id}.zip"
        if export.is_file() and _inside(export, settings.data_dir / "exports"):
            export.unlink()
    con.execute(
        """
        DELETE FROM clusters
        WHERE NOT EXISTS (
            SELECT 1 FROM cluster_members m
            WHERE m.run_id = clusters.run_id AND m.cluster_id = clusters.cluster_id
        )
        """
    )
    leftover = con.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM providers WHERE origin = 'demo' OR provider_key LIKE '199999%')
            + (SELECT COUNT(*) FROM leie WHERE origin = 'demo' OR npi LIKE '199999%')
            + (SELECT COUNT(*) FROM runs WHERE demo_only = TRUE)
        """
    ).fetchone()[0]
    if leftover:
        raise SystemExit(f"Rows outside the public extract remain ({leftover}).")
    if not keys and not run_ids:
        return "Public rows were kept."
    return (
        f"Removed {len(keys)} provider keys and {len(run_ids)} runs that were not part of a public extract. "
        "Public rows were kept."
    )
