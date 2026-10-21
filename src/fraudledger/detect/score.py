from __future__ import annotations

import duckdb

from fraudledger.settings import Settings


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
        slot = by_provider.setdefault(key, {"rules": set(), "excess": 0.0})
        slot["rules"].add(rule_id)
        if excess:
            slot["excess"] += float(excess)

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
        if len(members) < 2:
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
            score += 3.0
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
                slot["excess"],
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
