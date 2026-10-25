from __future__ import annotations

import math

import duckdb

from fraudledger.settings import Settings

# Added on top of R11's own weight. R13 does not receive it.
R11_BONUS = 3.0

# R07 and R12 describe one year-over-year jump. Count that jump once.
JUMP_ONCE = frozenset({"R07", "R12"})


def unduplicated_excess(rows) -> float:
    """Add peer-gap dollars, counting a payment jump a single time.

    R12 stores the same increase as R07 on older runs. The larger of those
    two figures is kept. Every other indicator is added on its own.
    """
    jump = 0.0
    other = 0.0
    for rule_id, excess in rows:
        try:
            amount = float(excess or 0)
        except (TypeError, ValueError):
            continue
        if amount <= 0:
            continue
        if rule_id in JUMP_ONCE:
            if amount > jump:
                jump = amount
        else:
            other += amount
    return other + jump


def gap_by_provider(con: duckdb.DuckDBPyConnection, run_id: str) -> dict[str, float]:
    grouped: dict[str, list] = {}
    for key, rule_id, excess in con.execute(
        """
        SELECT provider_key, rule_id, excess_dollars
        FROM flags
        WHERE run_id = ? AND excess_dollars IS NOT NULL AND excess_dollars <> 0
        """,
        [run_id],
    ).fetchall():
        grouped.setdefault(key, []).append((rule_id, excess))
    return {key: unduplicated_excess(items) for key, items in grouped.items()}


def recount_cluster(score: float, stored_excess: float | None, member_keys, gaps: dict[str, float]) -> tuple[float, float]:
    """Replace a stored group dollar total with the unduplicated one.

    The sort key's dollar term is the natural log of that total. Swap the
    term so an older run, which added a jump twice, does not keep the
    doubled figure on the page.
    """
    dollars = sum(float(gaps.get(key, 0) or 0) for key in member_keys)
    adjusted = (
        float(score or 0)
        - math.log1p(max(float(stored_excess or 0), 0.0))
        + math.log1p(max(dollars, 0.0))
    )
    return adjusted, dollars


def tier_for(score: float, rules: set[str], settings: Settings) -> str:
    if "R11" in rules or score >= settings.tier_critical:
        return "Critical"
    if score >= settings.tier_high:
        return "High"
    if score >= settings.tier_elevated:
        return "Elevated"
    if score > 0:
        return "Monitor"
    return "None"


def write_scores(con: duckdb.DuckDBPyConnection, settings: Settings, run_id: str) -> None:
    con.execute("DELETE FROM scores WHERE run_id = ?", [run_id])
    flag_rows = con.execute(
        """
        SELECT provider_key, rule_id, COALESCE(excess_dollars, 0)
        FROM flags
        WHERE run_id = ?
        """,
        [run_id],
    ).fetchall()
    by_provider: dict[str, dict] = {}
    for key, rule_id, excess in flag_rows:
        slot = by_provider.setdefault(key, {"rules": set(), "rows": []})
        slot["rules"].add(rule_id)
        slot["rows"].append((rule_id, excess))

    mates: dict[str, set[str]] = {}
    membership = con.execute(
        "SELECT cluster_id, provider_key FROM cluster_members WHERE run_id = ?",
        [run_id],
    ).fetchall()
    clusters: dict[int, list[str]] = {}
    for cluster_id, key in membership:
        clusters.setdefault(cluster_id, []).append(key)
    flagged = set(by_provider)
    for members in clusters.values():
        if len(members) < 2 or len(members) > settings.cluster_bonus_max_providers:
            continue
        flagged_members = [key for key in members if key in flagged]
        if len(flagged_members) < 2:
            continue
        for key in flagged_members:
            mates[key] = set(flagged_members) - {key}

    records = []
    for key, slot in by_provider.items():
        rules = slot["rules"]
        score = 0.0
        for rule_id in rules:
            score += float((settings.rules.get(rule_id) or {}).get("weight", 1))
        if len(rules) >= settings.multi_flag_min:
            score += settings.multi_flag_bonus
        if "R11" in rules:
            score += R11_BONUS
        if mates.get(key):
            score += settings.cluster_bonus
        ordered = ",".join(sorted(rules))
        records.append(
            (
                run_id,
                key,
                score,
                tier_for(score, rules, settings),
                len(rules),
                unduplicated_excess(slot["rows"]),
                ordered,
            )
        )
    if records:
        con.executemany(
            """
            INSERT INTO scores (run_id, provider_key, score, tier, flag_count, excess_dollars, rules)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            records,
        )

