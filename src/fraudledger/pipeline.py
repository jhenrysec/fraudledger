from __future__ import annotations

import subprocess
from datetime import datetime, timezone

import duckdb

from fraudledger.detect.engine import apply_rules
from fraudledger.detect.score import write_scores
from fraudledger.graph.cluster import build_communities
from fraudledger.report.assemble import write_report
from fraudledger.settings import Settings


def _git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def execute_run(con: duckdb.DuckDBPyConnection, settings: Settings, run_id: str | None = None) -> str:
    facts = con.execute("SELECT COUNT(*) FROM facts_medicaid").fetchone()[0]
    if not facts:
        raise SystemExit("No billing facts loaded. Load a public extract, then run again.")
    run_id = run_id or new_run_id()
    demo_only = con.execute(
        """
        SELECT COUNT(*) > 0 AND COUNT(*) = COUNT(*) FILTER (WHERE origin = 'demo')
        FROM facts_medicaid
        """
    ).fetchone()[0]
    con.execute(
        """
        INSERT OR REPLACE INTO runs (run_id, started_at, finished_at, git_sha, config_sha256, status, demo_only)
        VALUES (?, ?, NULL, ?, ?, 'running', ?)
        """,
        [run_id, datetime.now(timezone.utc).replace(tzinfo=None), _git_sha(), settings.config_sha256, bool(demo_only)],
    )
    apply_rules(con, settings, run_id)
    build_communities(con, settings, run_id)
    write_scores(con, settings, run_id)
    write_report(con, settings, run_id)
    con.execute(
        "UPDATE runs SET finished_at = ?, status = 'succeeded' WHERE run_id = ?",
        [datetime.now(timezone.utc).replace(tzinfo=None), run_id],
    )
    return run_id
