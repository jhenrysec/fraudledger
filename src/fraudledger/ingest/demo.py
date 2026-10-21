from __future__ import annotations

from datetime import date

import duckdb

from fraudledger.db import refresh_facts_view
from fraudledger.settings import Settings

# Fictional NPIs in a reserved demonstration range. Not real providers.
PEER_COUNT = 36


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

    # R06 flat billing at the peer rate, above the dollar floor, below the mill floor.
    for month in range(1, 9):
        add("1999990002", "T1019", 2024, month, 800, 50, 40)

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
