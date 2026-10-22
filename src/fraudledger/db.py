from __future__ import annotations

from pathlib import Path

import duckdb

SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id VARCHAR PRIMARY KEY,
    source_id VARCHAR,
    url VARCHAR,
    retrieved_at TIMESTAMP,
    byte_count BIGINT,
    sha256 VARCHAR,
    note VARCHAR
);

CREATE TABLE IF NOT EXISTS runs (
    run_id VARCHAR PRIMARY KEY,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    git_sha VARCHAR,
    config_sha256 VARCHAR,
    status VARCHAR,
    demo_only BOOLEAN
);

CREATE TABLE IF NOT EXISTS run_artifacts (
    run_id VARCHAR,
    path VARCHAR,
    sha256 VARCHAR,
    kind VARCHAR
);

CREATE TABLE IF NOT EXISTS providers (
    provider_key VARCHAR PRIMARY KEY,
    npi VARCHAR,
    enrollment_id VARCHAR,
    ccn VARCHAR,
    name VARCHAR,
    entity_type VARCHAR,
    taxonomy VARCHAR,
    address VARCHAR,
    city VARCHAR,
    state VARCHAR,
    zip VARCHAR,
    enumeration_date DATE,
    origin VARCHAR
);

CREATE TABLE IF NOT EXISTS leie (
    npi VARCHAR,
    name VARCHAR,
    excl_type VARCHAR,
    excl_date DATE,
    origin VARCHAR
);

CREATE TABLE IF NOT EXISTS ownership_edges (
    owner_key VARCHAR,
    owner_name VARCHAR,
    provider_key VARCHAR,
    percentage DOUBLE,
    role_code VARCHAR,
    role_strength DOUBLE,
    edge_weight DOUBLE,
    effective_date VARCHAR,
    edge_source_id VARCHAR,
    origin VARCHAR
);

CREATE TABLE IF NOT EXISTS flags (
    run_id VARCHAR,
    provider_key VARCHAR,
    rule_id VARCHAR,
    metric_value DOUBLE,
    threshold DOUBLE,
    peer_value DOUBLE,
    excess_dollars DOUBLE,
    detail_json VARCHAR
);

CREATE TABLE IF NOT EXISTS scores (
    run_id VARCHAR,
    provider_key VARCHAR,
    score DOUBLE,
    tier VARCHAR,
    flag_count INTEGER,
    excess_dollars DOUBLE,
    rules VARCHAR
);

CREATE TABLE IF NOT EXISTS clusters (
    run_id VARCHAR,
    cluster_id INTEGER,
    provider_count INTEGER,
    owner_count INTEGER,
    flagged_count INTEGER,
    score DOUBLE,
    excess_dollars DOUBLE
);

CREATE TABLE IF NOT EXISTS cluster_members (
    run_id VARCHAR,
    cluster_id INTEGER,
    provider_key VARCHAR
);

CREATE TABLE IF NOT EXISTS enrollments (
    enrollment_id VARCHAR,
    npi VARCHAR,
    ccn VARCHAR,
    name VARCHAR,
    address VARCHAR,
    city VARCHAR,
    state VARCHAR,
    zip VARCHAR,
    origin VARCHAR
);

CREATE TABLE IF NOT EXISTS review_states (
    provider_key VARCHAR PRIMARY KEY,
    state VARCHAR,
    note VARCHAR,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS medicare_use (
    ccn VARCHAR,
    setting VARCHAR,
    year INTEGER,
    stays DOUBLE,
    service_days DOUBLE,
    visits DOUBLE,
    rhc_days_pct DOUBLE,
    origin VARCHAR
);
"""

MIGRATIONS = (
    "ALTER TABLE providers ADD COLUMN IF NOT EXISTS incorporation_date DATE",
    "ALTER TABLE enrollments ADD COLUMN IF NOT EXISTS incorporation_date DATE",
    "ALTER TABLE leie ADD COLUMN IF NOT EXISTS first_name VARCHAR",
    "ALTER TABLE leie ADD COLUMN IF NOT EXISTS last_name VARCHAR",
    "ALTER TABLE leie ADD COLUMN IF NOT EXISTS state VARCHAR",
    "ALTER TABLE ownership_edges ADD COLUMN IF NOT EXISTS owner_first VARCHAR",
    "ALTER TABLE ownership_edges ADD COLUMN IF NOT EXISTS owner_last VARCHAR",
    "ALTER TABLE ownership_edges ADD COLUMN IF NOT EXISTS owner_state VARCHAR",
)


def connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    con.execute(SCHEMA)
    for statement in MIGRATIONS:
        con.execute(statement)
    return con


def refresh_facts_view(con: duckdb.DuckDBPyConnection, parquet_dir: Path) -> int:
    parquet_dir.mkdir(parents=True, exist_ok=True)
    files = sorted(parquet_dir.glob("*.parquet"))
    empty = """
        CREATE OR REPLACE VIEW facts_medicaid AS
        SELECT
            CAST(NULL AS VARCHAR) AS billing_npi,
            CAST(NULL AS VARCHAR) AS servicing_npi,
            CAST(NULL AS VARCHAR) AS hcpcs,
            CAST(NULL AS DATE) AS month,
            CAST(NULL AS DOUBLE) AS beneficiaries,
            CAST(NULL AS DOUBLE) AS claim_lines,
            CAST(NULL AS DOUBLE) AS paid,
            CAST(NULL AS VARCHAR) AS origin
        WHERE 1 = 0
    """
    if not files:
        con.execute(empty)
        return 0
    readable: list[Path] = []
    for path in files:
        try:
            con.execute("SELECT 1 FROM read_parquet(?) LIMIT 1", [str(path)])
        except Exception:
            path.replace(path.with_name(path.name + ".unreadable"))
            continue
        readable.append(path)
    if not readable:
        con.execute(empty)
        return 0
    listing = ", ".join("'" + path.as_posix().replace("'", "''") + "'" for path in readable)
    try:
        con.execute(
            f"""
            CREATE OR REPLACE VIEW facts_medicaid AS
            SELECT
                billing_npi::VARCHAR AS billing_npi,
                servicing_npi::VARCHAR AS servicing_npi,
                hcpcs::VARCHAR AS hcpcs,
                CAST(month AS DATE) AS month,
                beneficiaries::DOUBLE AS beneficiaries,
                claim_lines::DOUBLE AS claim_lines,
                paid::DOUBLE AS paid,
                origin::VARCHAR AS origin
            FROM read_parquet([{listing}])
            """
        )
    except Exception:
        con.execute(empty)
        return 0
    return len(readable)
