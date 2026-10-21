from __future__ import annotations

import json

import duckdb

from fraudledger.settings import Settings

RULE_BLURB = {
    "R01": "Paid per claim line is more than the configured multiple of the national median for that HCPCS, with enough dollars to matter.",
    "R02": "The provider is above the 90th percentile rate on at least two HCPCS codes that have a large enough peer group.",
    "R03": "Claim lines divided by the largest monthly beneficiary count exceed the configured cap. Monthly beneficiary counts are not additive across months, so this uses the max month.",
    "R04": "Average claim lines per active month imply more lines per working day than the configured cap.",
    "R05": "One or two HCPCS codes account for a very large paid total.",
    "R06": "Monthly paid amounts barely move across many months. Real demand usually varies.",
    "R07": "Paid jumped year over year by more than the configured ratio and by more than the configured dollar change.",
    "R08": "Latest calendar year paid grew by more than the explosive-growth ratio versus the prior calendar year.",
    "R09": "The first year of billing is large and the NPPES enumeration year is not older than that first billing year.",
    "R10": "The NPPES enumeration date is on or after the configured date and lifetime paid in this extract is very large.",
    "R11": "The billing NPI is on the OIG LEIE and payments appear on or after the exclusion date.",
}


def _rule(settings: Settings, rule_id: str) -> dict:
    return dict(settings.rules.get(rule_id) or {})


def _insert(con: duckdb.DuckDBPyConnection, run_id: str, rows: list[tuple]) -> None:
    if not rows:
        return
    con.executemany(
        """
        INSERT INTO flags (
            run_id, provider_key, rule_id, metric_value, threshold, peer_value, excess_dollars, detail_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [(run_id, *row) for row in rows],
    )


def apply_rules(con: duckdb.DuckDBPyConnection, settings: Settings, run_id: str) -> int:
    con.execute("DELETE FROM flags WHERE run_id = ?", [run_id])
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE agg_hcpcs AS
        SELECT
            billing_npi AS provider_key,
            hcpcs,
            SUM(paid) AS paid,
            SUM(claim_lines) AS claim_lines,
            CASE WHEN SUM(claim_lines) > 0 THEN SUM(paid) / SUM(claim_lines) ELSE NULL END AS rate
        FROM facts_medicaid
        GROUP BY 1, 2
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE benchmarks AS
        SELECT
            hcpcs,
            COUNT(*) AS n,
            MEDIAN(rate) AS median_rate,
            QUANTILE_CONT(rate, 0.9) AS p90
        FROM agg_hcpcs
        WHERE rate IS NOT NULL
        GROUP BY 1
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE agg_month AS
        SELECT billing_npi AS provider_key, month, SUM(paid) AS paid,
               SUM(claim_lines) AS claim_lines, MAX(beneficiaries) AS beneficiaries
        FROM facts_medicaid
        GROUP BY 1, 2
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE agg_year AS
        SELECT provider_key, year(month) AS year, SUM(paid) AS paid
        FROM agg_month
        GROUP BY 1, 2
        """
    )

    r01 = _rule(settings, "R01")
    multiple = float(r01.get("multiple_of_median", 3))
    min_paid = float(r01.get("min_paid", 100000))
    hits = con.execute(
        """
        SELECT provider_key, hcpcs, rate, paid, claim_lines, median_rate
        FROM agg_hcpcs
        JOIN benchmarks USING (hcpcs)
        WHERE n >= ? AND rate > median_rate * ? AND paid >= ?
        """,
        [settings.min_peer_n, multiple, min_paid],
    ).fetchall()
    _insert(
        con,
        run_id,
        [
            (
                key,
                "R01",
                rate,
                median * multiple,
                median,
                (rate - median) * lines,
                json.dumps(
                    {
                        "rule": "R01",
                        "hcpcs": hcpcs,
                        "rate": rate,
                        "median": median,
                        "lines": lines,
                        "paid": paid,
                        "formula": "(rate - national_median) * claim_lines",
                    }
                ),
            )
            for key, hcpcs, rate, paid, lines, median in hits
        ],
    )

    r02 = _rule(settings, "R02")
    min_codes = int(r02.get("min_codes", 2))
    code_hits = con.execute(
        """
        SELECT provider_key, COUNT(*) AS codes
        FROM agg_hcpcs
        JOIN benchmarks USING (hcpcs)
        WHERE n >= ? AND rate > p90
        GROUP BY 1
        HAVING COUNT(*) >= ?
        """,
        [settings.min_peer_n, min_codes],
    ).fetchall()
    _insert(
        con,
        run_id,
        [
            (
                key,
                "R02",
                codes,
                min_codes,
                None,
                None,
                json.dumps({"rule": "R02", "codes_above_p90": codes}),
            )
            for key, codes in code_hits
        ],
    )

    r03 = _rule(settings, "R03")
    cap = float(r03.get("max_lines_per_beneficiary", 100))
    intensity = con.execute(
        """
        SELECT provider_key,
               SUM(claim_lines) / NULLIF(MAX(beneficiaries), 0) AS ratio
        FROM agg_month
        GROUP BY 1
        HAVING ratio > ?
        """,
        [cap],
    ).fetchall()
    _insert(
        con,
        run_id,
        [
            (
                key,
                "R03",
                ratio,
                cap,
                None,
                None,
                json.dumps(
                    {
                        "rule": "R03",
                        "lines_per_max_monthly_beneficiaries": ratio,
                        "note": "Beneficiary counts are not additive across months; the denominator is the busiest month.",
                    }
                ),
            )
            for key, ratio in intensity
        ],
    )

    r04 = _rule(settings, "R04")
    per_day = float(r04.get("lines_per_working_day", 50))
    working = float(r04.get("working_days_per_month", 21))
    velocity = con.execute(
        """
        SELECT provider_key,
               SUM(claim_lines) / COUNT(*) / ? AS lines_per_day
        FROM agg_month
        GROUP BY 1
        HAVING lines_per_day >= ?
        """,
        [working, per_day],
    ).fetchall()
    _insert(
        con,
        run_id,
        [
            (
                key,
                "R04",
                lines,
                per_day,
                None,
                None,
                json.dumps({"rule": "R04", "lines_per_working_day": lines, "working_days_per_month": working}),
            )
            for key, lines in velocity
        ],
    )

    r05 = _rule(settings, "R05")
    max_codes = int(r05.get("max_codes", 2))
    mill_paid = float(r05.get("min_paid", 1_000_000))
    mills = con.execute(
        """
        SELECT provider_key, COUNT(*) AS codes, SUM(paid) AS paid
        FROM agg_hcpcs
        GROUP BY 1
        HAVING COUNT(*) <= ? AND SUM(paid) >= ?
        """,
        [max_codes, mill_paid],
    ).fetchall()
    _insert(
        con,
        run_id,
        [
            (
                key,
                "R05",
                codes,
                max_codes,
                None,
                None,
                json.dumps({"rule": "R05", "distinct_hcpcs": codes, "paid": paid}),
            )
            for key, codes, paid in mills
        ],
    )

    r06 = _rule(settings, "R06")
    max_cv = float(r06.get("max_cv", 0.10))
    min_months = int(r06.get("min_months", 6))
    flat_paid = float(r06.get("min_paid", 250000))
    flats = con.execute(
        """
        SELECT provider_key,
               STDDEV_SAMP(paid) / NULLIF(AVG(paid), 0) AS cv,
               COUNT(*) AS months,
               SUM(paid) AS paid
        FROM agg_month
        GROUP BY 1
        HAVING COUNT(*) >= ?
           AND SUM(paid) >= ?
           AND STDDEV_SAMP(paid) / NULLIF(AVG(paid), 0) < ?
        """,
        [min_months, flat_paid, max_cv],
    ).fetchall()
    _insert(
        con,
        run_id,
        [
            (
                key,
                "R06",
                cv,
                max_cv,
                None,
                None,
                json.dumps({"rule": "R06", "cv": cv, "months": months, "paid": paid}),
            )
            for key, cv, months, paid in flats
        ],
    )

    r07 = _rule(settings, "R07")
    r08 = _rule(settings, "R08")
    yoy = con.execute(
        """
        WITH ranked AS (
            SELECT provider_key, year, paid,
                   LAG(paid) OVER (PARTITION BY provider_key ORDER BY year) AS prior_paid,
                   LAG(year) OVER (PARTITION BY provider_key ORDER BY year) AS prior_year
            FROM agg_year
        )
        SELECT provider_key, year, paid, prior_year, prior_paid,
               (paid - prior_paid) / NULLIF(prior_paid, 0) AS growth
        FROM ranked
        WHERE prior_year = year - 1
          AND year = (SELECT MAX(year) FROM agg_year y2 WHERE y2.provider_key = ranked.provider_key)
        """
    ).fetchall()
    swing_ratio = float(r07.get("min_yoy", 2))
    swing_abs = float(r07.get("min_abs_change", 1_000_000))
    boom_ratio = float(r08.get("min_yoy", 5))
    boom_paid = float(r08.get("min_latest_paid", 250000))
    swing_rows = []
    boom_rows = []
    for key, year, paid, prior_year, prior_paid, growth in yoy:
        change = paid - prior_paid
        detail = {
            "year": int(year),
            "prior_year": int(prior_year),
            "paid": paid,
            "prior_paid": prior_paid,
            "growth": growth,
            "absolute_change": change,
        }
        if growth > swing_ratio and change >= swing_abs:
            swing_rows.append(
                (
                    key,
                    "R07",
                    growth,
                    swing_ratio,
                    prior_paid,
                    change,
                    json.dumps({"rule": "R07", **detail, "formula": "latest_year_paid - prior_year_paid"}),
                )
            )
        if growth > boom_ratio and paid >= boom_paid:
            boom_rows.append(
                (
                    key,
                    "R08",
                    growth,
                    boom_ratio,
                    prior_paid,
                    None,
                    json.dumps({"rule": "R08", **detail}),
                )
            )
    _insert(con, run_id, swing_rows)
    _insert(con, run_id, boom_rows)

    r09 = _rule(settings, "R09")
    first_min = float(r09.get("min_first_year_paid", 1_000_000))
    entrants = con.execute(
        """
        WITH firsts AS (
            SELECT provider_key, MIN(year) AS first_year FROM agg_year GROUP BY 1
        )
        SELECT y.provider_key, y.paid, f.first_year, p.enumeration_date
        FROM agg_year y
        JOIN firsts f ON y.provider_key = f.provider_key AND y.year = f.first_year
        LEFT JOIN providers p ON p.provider_key = y.provider_key
        WHERE y.paid >= ?
          AND (p.enumeration_date IS NULL OR year(p.enumeration_date) >= f.first_year)
        """,
        [first_min],
    ).fetchall()
    _insert(
        con,
        run_id,
        [
            (
                key,
                "R09",
                paid,
                first_min,
                None,
                None,
                json.dumps(
                    {
                        "rule": "R09",
                        "first_year": int(year),
                        "first_year_paid": paid,
                        "enumeration_date": str(enumerated) if enumerated else None,
                    }
                ),
            )
            for key, paid, year, enumerated in entrants
        ],
    )

    r10 = _rule(settings, "R10")
    enumerated_after = str(r10.get("enumeration_on_or_after", "2022-01-01"))
    huge = float(r10.get("min_paid", 5_000_000))
    newborns = con.execute(
        """
        SELECT m.provider_key, SUM(m.paid) AS paid, p.enumeration_date
        FROM agg_month m
        JOIN providers p ON p.provider_key = m.provider_key
        WHERE p.enumeration_date >= CAST(? AS DATE)
        GROUP BY 1, 3
        HAVING SUM(m.paid) >= ?
        """,
        [enumerated_after, huge],
    ).fetchall()
    _insert(
        con,
        run_id,
        [
            (
                key,
                "R10",
                paid,
                huge,
                None,
                None,
                json.dumps(
                    {
                        "rule": "R10",
                        "paid": paid,
                        "enumeration_date": str(enumerated),
                        "enumeration_on_or_after": enumerated_after,
                    }
                ),
            )
            for key, paid, enumerated in newborns
        ],
    )

    excluded = con.execute(
        """
        SELECT f.billing_npi,
               SUM(CASE WHEN l.excl_date IS NULL OR f.month >= l.excl_date THEN f.paid ELSE 0 END) AS paid_after,
               MIN(l.excl_date) AS excl_date,
               MIN(l.excl_type) AS excl_type
        FROM facts_medicaid f
        JOIN leie l ON l.npi = f.billing_npi
        GROUP BY 1
        HAVING paid_after > 0
        """
    ).fetchall()
    _insert(
        con,
        run_id,
        [
            (
                key,
                "R11",
                paid_after,
                0,
                None,
                paid_after,
                json.dumps(
                    {
                        "rule": "R11",
                        "paid_on_or_after_exclusion": paid_after,
                        "excl_date": str(excl_date) if excl_date else None,
                        "excl_type": excl_type,
                        "formula": "sum of paid where month >= exclusion date",
                    }
                ),
            )
            for key, paid_after, excl_date, excl_type in excluded
        ],
    )
    return con.execute("SELECT COUNT(*) FROM flags WHERE run_id = ?", [run_id]).fetchone()[0]
