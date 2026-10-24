from __future__ import annotations

import json

import duckdb

from fraudledger.ingest.focus import HOME_HEALTH_HCPCS, HOSPICE_HCPCS, PERSONAL_CARE_HCPCS
from fraudledger.settings import Settings

HOSPICE_TAXONOMY = ("251G00000X", "315D00000X")
# A paid jump is not flagged when patient growth is at least this share of the paid growth.
CENSUS_SHARE = 0.75
# A paid jump is not flagged when paid per patient grew by less than this share.
RATE_SHARE = 0.25

RULE_BLURB = {
    "R01": "Paid per claim line is more than the configured multiple of the peer median for that HCPCS. The peer group is the provider's state when that state has enough billers. Otherwise it is national.",
    "R02": "The provider is above the 90th percentile rate on at least two HCPCS codes that have a large enough peer group.",
    "R03": "Claim lines divided by the largest monthly beneficiary count exceed the configured cap. Monthly beneficiary counts are not additive across months, so this uses the max month.",
    "R04": "Average claim lines per active month imply more lines per working day than the configured cap.",
    "R05": "One or two HCPCS codes account for a very large paid total. A provider that bills only the expected codes for hospice, home health, or personal care is not flagged.",
    "R06": "Monthly paid amounts barely move across many months. Real demand usually varies. A provider that bills only the expected codes for hospice, home health, or personal care is not flagged.",
    "R07": "Paid jumped year over year by more than the configured ratio and by more than the configured dollar change. A jump that tracks the patient count, or that leaves paid per patient almost unchanged, is not flagged.",
    "R08": "Latest calendar year paid grew by more than the explosive-growth ratio versus the prior calendar year. A jump that tracks the patient count is not flagged.",
    "R09": "The first year of billing is large, and an incorporation date or an NPPES enumeration date says the organization is new. A missing date is not treated as new.",
    "R10": "Lifetime paid in this extract is very large and the organization, or the NPI when no incorporation date is present, is on or after the configured date.",
    "R11": "The billing NPI is on the OIG LEIE and payments appear on or after the exclusion date.",
    "R12": "Paid jumped by more than the billing-swing thresholds and a qualifying owner was associated in the latest year or the year before.",
    "R13": "A qualifying owner matches the LEIE on exact first name, last name, and state. A middle name is also required when both sides have one. A reinstated person is not flagged. The agency name is not compared.",
    "R14": "Medicare hospice service days divided by stays are more than the configured multiple of the provider median. The certification number is matched with the enrollment state when both files have a state.",
    "R15": "Medicare home health visits per episode are more than the configured multiple of the provider median.",
    "R16": "A servicing NPI, not the billing NPI, is on the OIG LEIE and payments appear on or after the exclusion date.",
    "R17": "The same servicing NPI is paid through several billing NPIs that do not share an owner.",
    "R18": "Medicare Care Compare early or late live discharges are more than the configured multiple of the hospice median.",
    "R19": "Medicare Care Compare potentially preventable readmissions are more than the configured multiple of the home-health median.",
}


def _rule(settings: Settings, rule_id: str) -> dict:
    return dict(settings.rules.get(rule_id) or {})


def _family_sql(codes: frozenset[str]) -> str:
    listed = ", ".join(f"'{code}'" for code in sorted(codes))
    return f"""
        SELECT provider_key
        FROM agg_hcpcs
        GROUP BY 1
        HAVING SUM(CASE WHEN upper(hcpcs) NOT IN ({listed}) THEN 1 ELSE 0 END) = 0
    """


def _expected_hospice_sql(provider_key: str = "provider_key") -> str:
    """Providers whose narrow or flat billing is the expected product, not a lead."""
    taxes = ", ".join(f"'{code}'" for code in HOSPICE_TAXONOMY)
    hospice_codes = ", ".join(f"'{code}'" for code in sorted(HOSPICE_HCPCS))
    return f"""
        {provider_key} NOT IN (
            SELECT p.provider_key
            FROM providers p
            WHERE upper(COALESCE(p.taxonomy, '')) IN ({taxes})
              AND NOT EXISTS (
                  SELECT 1 FROM agg_hcpcs other
                  WHERE other.provider_key = p.provider_key
                    AND upper(other.hcpcs) NOT IN ({hospice_codes})
              )
            UNION
            {_family_sql(HOME_HEALTH_HCPCS)}
            UNION
            {_family_sql(PERSONAL_CARE_HCPCS)}
        )
    """


def _census_explains(paid_growth: float, beneficiaries, prior_beneficiaries) -> bool:
    if beneficiaries is None or prior_beneficiaries is None or paid_growth <= 0:
        return False
    prior = float(prior_beneficiaries)
    if prior <= 0:
        return False
    return (float(beneficiaries) - prior) / prior >= paid_growth * CENSUS_SHARE


def _rate_unchanged(paid: float, prior_paid: float, beneficiaries, prior_beneficiaries) -> bool:
    if beneficiaries is None or prior_beneficiaries is None or prior_paid <= 0:
        return False
    current_patients = float(beneficiaries)
    prior_patients = float(prior_beneficiaries)
    if current_patients <= 0 or prior_patients <= 0:
        return False
    prior_rate = prior_paid / prior_patients
    if prior_rate <= 0:
        return False
    current_rate = paid / current_patients
    return (current_rate - prior_rate) / prior_rate < RATE_SHARE


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
            f.billing_npi AS provider_key,
            upper(trim(f.hcpcs)) AS hcpcs,
            NULLIF(upper(trim(p.state)), '') AS state,
            SUM(f.paid) AS paid,
            SUM(f.claim_lines) AS claim_lines,
            CASE WHEN SUM(f.claim_lines) > 0 THEN SUM(f.paid) / SUM(f.claim_lines) ELSE NULL END AS rate
        FROM facts_medicaid f
        LEFT JOIN providers p ON p.provider_key = f.billing_npi
        WHERE f.billing_npi IS NOT NULL AND trim(f.billing_npi) <> ''
        GROUP BY 1, 2, 3
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
        CREATE OR REPLACE TEMP TABLE state_benchmarks AS
        SELECT
            state,
            hcpcs,
            COUNT(*) AS n,
            MEDIAN(rate) AS median_rate,
            QUANTILE_CONT(rate, 0.9) AS p90
        FROM agg_hcpcs
        WHERE rate IS NOT NULL AND state IS NOT NULL
        GROUP BY 1, 2
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE agg_month AS
        SELECT billing_npi AS provider_key, month, SUM(paid) AS paid,
               SUM(claim_lines) AS claim_lines, MAX(beneficiaries) AS beneficiaries
        FROM facts_medicaid
        WHERE billing_npi IS NOT NULL AND trim(billing_npi) <> ''
        GROUP BY 1, 2
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE agg_year AS
        SELECT provider_key, year(month) AS year, SUM(paid) AS paid,
               AVG(beneficiaries) AS beneficiaries
        FROM agg_month
        GROUP BY 1, 2
        """
    )

    r01 = _rule(settings, "R01")
    multiple = float(r01.get("multiple_of_median", 3))
    min_paid = float(r01.get("min_paid", 100000))
    hits = con.execute(
        """
        SELECT provider_key, hcpcs, rate, paid, claim_lines,
               CASE WHEN s.n >= ? THEN s.median_rate ELSE b.median_rate END AS median_rate,
               CASE WHEN s.n >= ? THEN 'state' ELSE 'national' END AS peer_scope,
               state
        FROM agg_hcpcs a
        JOIN benchmarks b USING (hcpcs)
        LEFT JOIN state_benchmarks s USING (state, hcpcs)
        WHERE paid >= ?
          AND (
            (s.n >= ? AND rate > s.median_rate * ?)
            OR ((s.n IS NULL OR s.n < ?) AND b.n >= ? AND rate > b.median_rate * ?)
          )
        """,
        [settings.min_peer_n, settings.min_peer_n, min_paid, settings.min_peer_n, multiple, settings.min_peer_n, settings.min_peer_n, multiple],
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
                        "peer_scope": scope,
                        "state": state,
                        "formula": "(rate - peer_median) * claim_lines",
                    }
                ),
            )
            for key, hcpcs, rate, paid, lines, median, scope, state in hits
        ],
    )

    r02 = _rule(settings, "R02")
    min_codes = int(r02.get("min_codes", 2))
    code_hits = con.execute(
        """
        SELECT a.provider_key, COUNT(*) AS codes
        FROM agg_hcpcs a
        JOIN benchmarks b USING (hcpcs)
        LEFT JOIN state_benchmarks s USING (state, hcpcs)
        WHERE (
            (s.n >= ? AND a.rate > s.p90)
            OR ((s.n IS NULL OR s.n < ?) AND b.n >= ? AND a.rate > b.p90)
          )
        GROUP BY 1
        HAVING COUNT(*) >= ?
        """,
        [settings.min_peer_n, settings.min_peer_n, settings.min_peer_n, min_codes],
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
    hospice_only = _expected_hospice_sql()
    mills = con.execute(
        f"""
        SELECT provider_key, COUNT(*) AS codes, SUM(paid) AS paid
        FROM agg_hcpcs
        GROUP BY 1
        HAVING COUNT(*) <= ? AND SUM(paid) >= ?
          AND {hospice_only}
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
        f"""
        SELECT provider_key,
               STDDEV_SAMP(paid) / NULLIF(AVG(paid), 0) AS cv,
               COUNT(*) AS months,
               SUM(paid) AS paid
        FROM agg_month
        GROUP BY 1
        HAVING COUNT(*) >= ?
           AND SUM(paid) >= ?
           AND STDDEV_SAMP(paid) / NULLIF(AVG(paid), 0) < ?
           AND {hospice_only}
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
            SELECT provider_key, year, paid, beneficiaries,
                   LAG(paid) OVER (PARTITION BY provider_key ORDER BY year) AS prior_paid,
                   LAG(year) OVER (PARTITION BY provider_key ORDER BY year) AS prior_year,
                   LAG(beneficiaries) OVER (PARTITION BY provider_key ORDER BY year) AS prior_beneficiaries
            FROM agg_year
        )
        SELECT provider_key, year, paid, prior_year, prior_paid, beneficiaries, prior_beneficiaries,
               (paid - prior_paid) / NULLIF(prior_paid, 0) AS growth
        FROM ranked
        WHERE prior_year = year - 1
          AND paid IS NOT NULL
          AND prior_paid IS NOT NULL
          AND prior_paid <> 0
          AND year = (SELECT MAX(year) FROM agg_year y2 WHERE y2.provider_key = ranked.provider_key)
        """
    ).fetchall()
    swing_ratio = float(r07.get("min_yoy", 2))
    swing_abs = float(r07.get("min_abs_change", 1_000_000))
    boom_ratio = float(r08.get("min_yoy", 5))
    boom_paid = float(r08.get("min_latest_paid", 250000))
    owner_years: dict[str, set[int]] = {}
    for provider_key, associated in con.execute(
        """
        SELECT provider_key, try_cast(effective_date AS DATE)
        FROM ownership_edges
        WHERE effective_date IS NOT NULL AND effective_date <> ''
        """
    ).fetchall():
        if associated is not None:
            owner_years.setdefault(provider_key, set()).add(associated.year)
    swing_rows = []
    boom_rows = []
    owner_rows = []
    for key, year, paid, prior_year, prior_paid, beneficiaries, prior_beneficiaries, growth in yoy:
        if None in (growth, paid, prior_paid, year, prior_year):
            continue
        paid = float(paid)
        prior_paid = float(prior_paid)
        growth = float(growth)
        change = paid - prior_paid
        if _census_explains(growth, beneficiaries, prior_beneficiaries):
            continue
        if _rate_unchanged(paid, prior_paid, beneficiaries, prior_beneficiaries):
            continue
        patient_growth = None
        if beneficiaries is not None and prior_beneficiaries not in (None, 0):
            patient_growth = (float(beneficiaries) - float(prior_beneficiaries)) / float(prior_beneficiaries)
        detail = {
            "year": int(year),
            "prior_year": int(prior_year),
            "paid": paid,
            "prior_paid": prior_paid,
            "growth": growth,
            "absolute_change": change,
            "patients": None if beneficiaries is None else float(beneficiaries),
            "prior_patients": None if prior_beneficiaries is None else float(prior_beneficiaries),
            "patient_growth": patient_growth,
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
        if growth > swing_ratio and change >= swing_abs and {int(year), int(year) - 1} & owner_years.get(key, set()):
            owner_rows.append(
                (
                    key,
                    "R12",
                    growth,
                    swing_ratio,
                    prior_paid,
                    change,
                    json.dumps({"rule": "R12", **detail, "owner_years": sorted(owner_years.get(key, set()))}),
                )
            )
    _insert(con, run_id, swing_rows)
    _insert(con, run_id, boom_rows)
    _insert(con, run_id, owner_rows)

    r09 = _rule(settings, "R09")
    first_min = float(r09.get("min_first_year_paid", 1_000_000))
    entrants = con.execute(
        """
        WITH firsts AS (
            SELECT provider_key, MIN(year) AS first_year FROM agg_year GROUP BY 1
        )
        SELECT y.provider_key, y.paid, f.first_year, p.enumeration_date, p.incorporation_date
        FROM agg_year y
        JOIN firsts f ON y.provider_key = f.provider_key AND y.year = f.first_year
        LEFT JOIN providers p ON p.provider_key = y.provider_key
        WHERE y.paid >= ?
          AND (
            (p.incorporation_date IS NOT NULL AND year(p.incorporation_date) >= f.first_year - 1)
            OR (
              p.incorporation_date IS NULL
              AND p.enumeration_date IS NOT NULL
              AND year(p.enumeration_date) >= f.first_year
            )
          )
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
                        "incorporation_date": str(incorporated) if incorporated else None,
                    }
                ),
            )
            for key, paid, year, enumerated, incorporated in entrants
        ],
    )

    r10 = _rule(settings, "R10")
    enumerated_after = str(r10.get("enumeration_on_or_after", "2022-01-01"))
    huge = float(r10.get("min_paid", 5_000_000))
    newborns = con.execute(
        """
        SELECT m.provider_key, SUM(m.paid) AS paid, p.enumeration_date, p.incorporation_date
        FROM agg_month m
        JOIN providers p ON p.provider_key = m.provider_key
        WHERE p.incorporation_date >= CAST(? AS DATE)
           OR (p.incorporation_date IS NULL AND p.enumeration_date >= CAST(? AS DATE))
        GROUP BY 1, 3, 4
        HAVING SUM(m.paid) >= ?
        """,
        [enumerated_after, enumerated_after, huge],
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
                        "enumeration_date": str(enumerated) if enumerated else None,
                        "incorporation_date": str(incorporated) if incorporated else None,
                        "enumeration_on_or_after": enumerated_after,
                    }
                ),
            )
            for key, paid, enumerated, incorporated in newborns
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

    owners = con.execute(
        """
        SELECT o.provider_key,
               MIN(o.owner_last),
               MIN(o.owner_first),
               MIN(o.owner_state),
               MIN(l.excl_date),
               MIN(l.excl_type),
               MAX(paid.paid) AS paid
        FROM ownership_edges o
        JOIN leie l
          ON l.first_name = o.owner_first
         AND l.last_name = o.owner_last
         AND l.state = o.owner_state
         AND (
            o.owner_middle IS NULL OR l.middle_name IS NULL
            OR o.owner_middle = l.middle_name
            OR (length(o.owner_middle) = 1 AND left(l.middle_name, 1) = o.owner_middle)
            OR (length(l.middle_name) = 1 AND left(o.owner_middle, 1) = l.middle_name)
         )
         AND (
            l.reinstated_on IS NULL
            OR l.reinstated_on > (
                SELECT MAX(month) FROM facts_medicaid f WHERE f.billing_npi = o.provider_key
            )
         )
        JOIN (
            SELECT billing_npi AS provider_key, SUM(paid) AS paid
            FROM facts_medicaid
            GROUP BY 1
        ) paid ON paid.provider_key = o.provider_key
        WHERE o.owner_first IS NOT NULL
          AND o.owner_last IS NOT NULL
          AND length(o.owner_last) >= 3
          AND length(o.owner_first) >= 2
          AND o.owner_state IS NOT NULL
          AND (l.npi IS NULL OR l.npi <> o.provider_key)
        GROUP BY 1
        HAVING MAX(paid.paid) > 0
        """
    ).fetchall()
    _insert(
        con,
        run_id,
        [
            (
                key,
                "R13",
                paid,
                0,
                None,
                None,
                json.dumps(
                    {
                        "rule": "R13",
                        "owner_last": last,
                        "owner_first": first,
                        "owner_state": state,
                        "excl_date": str(excl_date) if excl_date else None,
                        "excl_type": excl_type,
                        "paid": paid,
                        "match": "exact first name, last name, and state",
                    }
                ),
            )
            for key, last, first, state, excl_date, excl_type, paid in owners
        ],
    )

    def _peer_use(setting: str, ratio_sql: str, minimum: float, peers: int, multiple: float, rule_id: str) -> None:
        rows = con.execute(
            f"""
            WITH linked AS (
                SELECT e.npi AS provider_key,
                       m.stays,
                       {ratio_sql} AS metric,
                       m.rhc_days_pct
                FROM medicare_use m
                JOIN enrollments e
                  ON lpad(right(regexp_replace(COALESCE(e.ccn, ''), '[^0-9]', '', 'g'), 6), 6, '0')
                   = lpad(right(regexp_replace(COALESCE(m.ccn, ''), '[^0-9]', '', 'g'), 6), 6, '0')
                 AND (
                    NULLIF(upper(trim(COALESCE(e.state, ''))), '') IS NULL
                    OR NULLIF(upper(trim(COALESCE(m.state, ''))), '') IS NULL
                    OR upper(trim(e.state)) = upper(trim(m.state))
                 )
                WHERE m.setting = ?
                  AND m.stays >= ?
                  AND e.npi IN (SELECT provider_key FROM agg_year)
            ),
            bench AS (
                SELECT median(metric) AS median_metric, count(*) AS n
                FROM linked
                WHERE metric IS NOT NULL
            )
            SELECT provider_key, metric, stays, median_metric, rhc_days_pct
            FROM linked, bench
            WHERE n >= ? AND metric > median_metric * ?
            """,
            [setting, minimum, peers, multiple],
        ).fetchall()
        _insert(
            con,
            run_id,
            [
                (
                    key,
                    rule_id,
                    metric,
                    None if median is None else float(median) * multiple,
                    median,
                    None,
                    json.dumps(
                        {
                            "rule": rule_id,
                            "metric": metric,
                            "stays_or_episodes": stays,
                            "median": median,
                            "rhc_days_pct": rhc,
                        }
                    ),
                )
                for key, metric, stays, median, rhc in rows
            ],
        )

    r14 = _rule(settings, "R14")
    _peer_use(
        "hospice",
        "m.service_days / NULLIF(m.stays, 0)",
        float(r14.get("min_stays", 30)),
        int(r14.get("min_providers", 8)),
        float(r14.get("multiple_of_median", 2)),
        "R14",
    )
    r15 = _rule(settings, "R15")
    _peer_use(
        "home-health",
        "m.visits / NULLIF(m.stays, 0)",
        float(r15.get("min_episodes", 30)),
        int(r15.get("min_providers", 8)),
        float(r15.get("multiple_of_median", 2)),
        "R15",
    )

    r16_rows = con.execute(
        """
        SELECT f.billing_npi,
               f.servicing_npi,
               SUM(f.paid) AS paid_after,
               MIN(l.excl_date) AS excl_date
        FROM facts_medicaid f
        JOIN leie l ON l.npi = f.servicing_npi
        WHERE f.servicing_npi IS NOT NULL
          AND length(f.servicing_npi) = 10
          AND f.servicing_npi <> f.billing_npi
          AND (l.excl_date IS NULL OR f.month >= l.excl_date)
          AND NOT EXISTS (SELECT 1 FROM leie b WHERE b.npi = f.billing_npi)
        GROUP BY 1, 2
        HAVING paid_after > 0
        """
    ).fetchall()
    _insert(
        con,
        run_id,
        [
            (
                key,
                "R16",
                paid_after,
                0,
                None,
                paid_after,
                json.dumps(
                    {
                        "rule": "R16",
                        "servicing_npi": servicing,
                        "paid_on_or_after_exclusion": paid_after,
                        "excl_date": str(excl_date) if excl_date else None,
                    }
                ),
            )
            for key, servicing, paid_after, excl_date in r16_rows
        ],
    )

    r17 = _rule(settings, "R17")
    min_billers = int(r17.get("min_billers", 3))
    r17_paid = float(r17.get("min_paid", 250000))
    scattered = con.execute(
        """
        WITH pairs AS (
            SELECT billing_npi, servicing_npi, SUM(paid) AS paid
            FROM facts_medicaid
            WHERE servicing_npi IS NOT NULL
              AND length(servicing_npi) = 10
              AND servicing_npi <> billing_npi
            GROUP BY 1, 2
        )
        SELECT a.billing_npi,
               a.servicing_npi,
               COUNT(DISTINCT b.billing_npi) + 1 AS billers,
               SUM(DISTINCT a.paid) AS paid
        FROM pairs a
        JOIN pairs b
          ON a.servicing_npi = b.servicing_npi
         AND a.billing_npi <> b.billing_npi
        WHERE NOT EXISTS (
            SELECT 1
            FROM ownership_edges left_owner
            JOIN ownership_edges right_owner ON left_owner.owner_key = right_owner.owner_key
            WHERE left_owner.provider_key = a.billing_npi
              AND right_owner.provider_key = b.billing_npi
        )
        GROUP BY 1, 2
        HAVING COUNT(DISTINCT b.billing_npi) + 1 >= ?
           AND SUM(DISTINCT a.paid) >= ?
        """,
        [min_billers, r17_paid],
    ).fetchall()
    _insert(
        con,
        run_id,
        [
            (
                key,
                "R17",
                billers,
                min_billers,
                None,
                None,
                json.dumps({"rule": "R17", "servicing_npi": servicing, "billers": billers, "paid": paid}),
            )
            for key, servicing, billers, paid in scattered
        ],
    )

    def _care_peer(setting: str, measures: tuple[str, ...], multiple: float, peers: int, rule_id: str) -> None:
        placeholders = ", ".join("?" for _ in measures)
        rows = con.execute(
            f"""
            WITH linked AS (
                SELECT e.npi AS provider_key, c.measure, c.score AS metric
                FROM care_compare c
                JOIN enrollments e
                  ON lpad(right(regexp_replace(COALESCE(e.ccn, ''), '[^0-9]', '', 'g'), 6), 6, '0')
                   = lpad(right(regexp_replace(COALESCE(c.ccn, ''), '[^0-9]', '', 'g'), 6), 6, '0')
                 AND (
                    NULLIF(upper(trim(COALESCE(e.state, ''))), '') IS NULL
                    OR NULLIF(upper(trim(COALESCE(c.state, ''))), '') IS NULL
                    OR upper(trim(e.state)) = upper(trim(c.state))
                 )
                WHERE c.setting = ?
                  AND c.measure IN ({placeholders})
                  AND c.score IS NOT NULL
                  AND e.npi IN (SELECT provider_key FROM agg_year)
            ),
            bench AS (
                SELECT measure, median(metric) AS median_metric, count(*) AS n
                FROM linked
                GROUP BY 1
            )
            SELECT provider_key, measure, metric, median_metric
            FROM linked
            JOIN bench USING (measure)
            WHERE n >= ? AND metric > median_metric * ?
            """,
            [setting, *measures, peers, multiple],
        ).fetchall()
        _insert(
            con,
            run_id,
            [
                (
                    key,
                    rule_id,
                    float(metric),
                    None if median is None else float(median) * multiple,
                    None if median is None else float(median),
                    None,
                    json.dumps({"rule": rule_id, "measure": measure, "score": float(metric)}),
                )
                for key, measure, metric, median in rows
            ],
        )

    r18 = _rule(settings, "R18")
    _care_peer(
        "hospice",
        ("Early live discharges", "Late live discharges"),
        float(r18.get("multiple_of_median", 2)),
        int(r18.get("min_providers", 8)),
        "R18",
    )
    r19 = _rule(settings, "R19")
    _care_peer(
        "home-health",
        ("PPR Risk-Standardized Rate",),
        float(r19.get("multiple_of_median", 2)),
        int(r19.get("min_providers", 8)),
        "R19",
    )
    return con.execute("SELECT COUNT(*) FROM flags WHERE run_id = ?", [run_id]).fetchone()[0]
