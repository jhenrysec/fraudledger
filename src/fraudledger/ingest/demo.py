from __future__ import annotations

import re
import shutil
from datetime import date

import duckdb

from fraudledger.db import refresh_facts_view
from fraudledger.settings import Settings

# Fictional NPIs in a reserved demonstration range. Not real providers.
PEER_COUNT = 36
_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")


def _month(year: int, month: int) -> date:
    return date(year, month, 1)


def load_demo(con: duckdb.DuckDBPyConnection, settings: Settings) -> None:
    """Replace demonstration rows with a deterministic fixture and write Parquet."""
    settings.ensure_dirs()
    con.execute("DELETE FROM providers WHERE origin = 'demo'")
    con.execute("DELETE FROM leie WHERE origin = 'demo'")
    con.execute("DELETE FROM ownership_edges WHERE origin = 'demo'")

    rows: list[tuple] = []

    def add(
        npi: str,
        hcpcs: str,
        year: int,
        month: int,
        lines: float,
        rate: float,
        beneficiaries: float,
    ) -> None:
        rows.append(
            (
                npi,
                npi,
                hcpcs,
                _month(year, month),
                beneficiaries,
                lines,
                round(lines * rate, 2),
                "demo",
            )
        )

    for index in range(PEER_COUNT):
        npi = f"1999991{index:03d}"
        for month in range(1, 13):
            add(npi, "T1019", 2024, month, 100, 50, 20)

    # R01 cost outlier. Rates stay above 3x median; monthly paid varies so CV > 0.10.
    outlier_rates = [280, 320, 260, 340, 300, 360]
    for month, rate in enumerate(outlier_rates, start=1):
        add("1999990001", "T1019", 2024, month, 400, rate, 40)

    # R06 flat billing at the peer rate. The second code keeps this from looking like
    # ordinary personal-care billing, which is no longer flagged for being narrow or flat.
    for month in range(1, 9):
        add("1999990002", "T1019", 2024, month, 800, 50, 40)
        add("1999990002", "T2042", 2024, month, 20, 50, 40)

    # R11 excluded, otherwise ordinary and small.
    for month in range(1, 7):
        add("1999990003", "T1019", 2024, month, 100, 50, 20)

    # Clean control.
    for month, rate in enumerate([46, 54, 48, 52, 47, 53, 49, 51, 45, 55, 50, 50], start=1):
        add("1999990004", "T1019", 2024, month, 80, rate, 20)

    # R09 new entrant. First year exceeds $1M at the peer rate, so daily velocity also trips.
    for month in range(1, 13):
        add("1999990005", "T1019", 2024, month, 1700, 50, 80)
    for month in range(1, 7):
        add("1999990005", "T1019", 2025, month, 200, 50, 40)

    # R04 velocity, single busy month, normal rate.
    add("1999990006", "T1019", 2024, 6, 2100, 50, 400)

    # R07 swing. 2024 volume is high enough that R04 may also fire; that is expected.
    for month in range(1, 13):
        add("1999990007", "T1019", 2023, month, 667, 50, 40)
    for month in range(1, 13):
        add("1999990007", "T1019", 2024, month, 2834, 50, 80)

    # R08 explosive growth, latest year under the velocity cap.
    for month in range(1, 5):
        add("1999990008", "T1019", 2023, month, 200, 50, 20)
    for month in range(1, 11):
        add("1999990008", "T1019", 2024, month, 800, 50, 40)

    # R03 high lines per beneficiary, under the daily velocity cap.
    add("1999990009", "T1019", 2024, 4, 1000, 50, 5)

    # Cluster A: cost outlier inside an ownership group.
    for month in range(1, 7):
        add("1999990010", "T1019", 2024, month, 200, 200, 20)
    # Cluster B: flat biller inside the same group.
    for month in range(1, 9):
        add("1999990011", "T1019", 2024, month, 800, 50, 40)
    # Cluster C: ordinary sibling.
    for month in range(1, 13):
        add("1999990012", "T1019", 2024, month, 90, 50, 20)

    # R10 very new, very large. Extra rules may co-fire; R10 must be present.
    for year in (2023, 2024, 2025):
        for month in range(1, 13):
            add("1999990013", "T1019", year, month, 3056, 50, 100)

    con.execute("CREATE TEMP TABLE demo_seed (billing_npi VARCHAR, servicing_npi VARCHAR, hcpcs VARCHAR, month DATE, beneficiaries DOUBLE, claim_lines DOUBLE, paid DOUBLE, origin VARCHAR)")
    con.executemany("INSERT INTO demo_seed VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    parquet = settings.data_dir / "parquet" / "facts" / "demo.parquet"
    parquet.parent.mkdir(parents=True, exist_ok=True)
    if parquet.exists():
        parquet.unlink()
    target = "'" + parquet.as_posix().replace("'", "''") + "'"
    con.execute(f"COPY (SELECT * FROM demo_seed) TO {target} (FORMAT PARQUET)")
    con.execute("DROP TABLE demo_seed")
    refresh_facts_view(con, settings.data_dir / "parquet" / "facts")

    providers = [
        ("1999990001", "Northwind Personal Care LLC", "2016-04-01", "100 OAK ST", "AUSTIN", "TX"),
        ("1999990002", "Plainfield Attendant Services", "2015-02-01", "200 ELM ST", "AUSTIN", "TX"),
        ("1999990003", "Excluded Example Physician", "2012-01-01", "300 PINE ST", "AUSTIN", "TX"),
        ("1999990004", "Harbor Family Practice", "2015-06-01", "400 CEDAR ST", "AUSTIN", "TX"),
        ("1999990005", "First Year Home Health LLC", "2024-01-15", "500 MAPLE ST", "DALLAS", "TX"),
        ("1999990006", "Rapid Visit Agency", "2018-03-01", "600 BIRCH ST", "DALLAS", "TX"),
        ("1999990007", "Step Change Home Care", "2014-07-01", "700 WALNUT ST", "HOUSTON", "TX"),
        ("1999990008", "Spike Billing LLC", "2016-08-01", "800 ASH ST", "HOUSTON", "TX"),
        ("1999990009", "High Intensity Clinic", "2017-09-01", "900 WILLOW ST", "SAN ANTONIO", "TX"),
        ("1999990010", "Cluster Hospice Alpha", "2019-01-01", "10 SHARED WAY", "PHOENIX", "AZ"),
        ("1999990011", "Cluster Hospice Bravo", "2019-02-01", "10 SHARED WAY", "PHOENIX", "AZ"),
        ("1999990012", "Cluster Hospice Charlie", "2019-03-01", "10 SHARED WAY", "PHOENIX", "AZ"),
        ("1999990013", "Legacy Scale Provider", "2023-01-15", "15 SCALE AVE", "TUCSON", "AZ"),
    ]
    for index in range(PEER_COUNT):
        npi = f"1999991{index:03d}"
        providers.append(
            (npi, f"Peer Practice {index:02d}", "2018-01-01", f"{1000 + index} PEER ST", "AUSTIN", "TX")
        )
    con.executemany(
        """
        INSERT INTO providers (
            provider_key, npi, enrollment_id, ccn, name, entity_type, taxonomy,
            address, city, state, zip, enumeration_date, origin
        ) VALUES (?, ?, ?, NULL, ?, 'organization', '251E00000X', ?, ?, ?, '00000', ?, 'demo')
        """,
        [
            (npi, npi, "ENR" + npi[-4:], name, address, city, state, date.fromisoformat(enumerated))
            for npi, name, enumerated, address, city, state in providers
        ],
    )
    con.execute(
        """
        INSERT INTO leie (npi, name, excl_type, excl_date, origin)
        VALUES ('1999990003', 'Excluded Example Physician', '1128a1', DATE '2020-01-01', 'demo')
        """
    )
    for npi in ("1999990010", "1999990011", "1999990012"):
        con.execute(
            """
            INSERT INTO ownership_edges (
                owner_key, owner_name, provider_key, percentage, role_code,
                role_strength, edge_weight, effective_date, edge_source_id, origin
            ) VALUES ('A100', 'Shared Control LLC', ?, 100, '40', 3.0, 3.0, '2019-01-01', ?, 'demo')
            """,
            [npi, f"ENR-{npi}-A100-40"],
        )


def _inside(path, root) -> bool:
    resolved = path.resolve()
    base = root.resolve()
    return resolved != base and base in resolved.parents


def purge_demo(con: duckdb.DuckDBPyConnection, settings: Settings) -> str:
    """Remove fictional rows, the demonstration parquet, and demonstration runs.

    Public extracts are left in place.
    """
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
        for row in con.execute(
            "SELECT run_id FROM runs WHERE demo_only = TRUE"
        ).fetchall()
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
        raise SystemExit(f"Demonstration rows remain after the purge ({leftover}).")
    return (
        f"Removed demonstration data. {len(keys)} fictional provider keys, "
        f"{len(run_ids)} demonstration runs. Public rows were not deleted."
    )
