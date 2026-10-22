"""Owners and street addresses that connect more than one provider in a group."""

from __future__ import annotations

import duckdb


def _who(providers: list[dict], limit: int = 8) -> str:
    names = [item["name"] or item["key"] for item in providers]
    if len(names) <= limit:
        return ", ".join(names)
    rest = len(names) - limit
    return ", ".join(names[:limit]) + f", and {rest} more"


def _place(address: str, city: str, state: str) -> str:
    return ", ".join(part for part in (address, city, state) if part)


def shared_links(con: duckdb.DuckDBPyConnection, run_id: str) -> dict[int, dict]:
    """Owners and addresses on at least two providers in the same group."""
    owners: dict[int, dict[str, dict]] = {}
    for cluster_id, owner_key, owner_name, provider_key, provider_name in con.execute(
        """
        SELECT m.cluster_id, e.owner_key, e.owner_name, e.provider_key, p.name
        FROM cluster_members m
        JOIN ownership_edges e ON e.provider_key = m.provider_key
        LEFT JOIN providers p ON p.provider_key = e.provider_key
        WHERE m.run_id = ?
        """,
        [run_id],
    ).fetchall():
        slot = owners.setdefault(cluster_id, {}).setdefault(
            owner_key,
            {"key": owner_key, "name": "", "providers": {}},
        )
        name = (owner_name or "").strip()
        if len(name) > len(slot["name"]):
            slot["name"] = name
        slot["providers"][provider_key] = (provider_name or "").strip()

    addresses: dict[int, dict[tuple, dict]] = {}
    for cluster_id, address, city, state, provider_key, provider_name in con.execute(
        """
        SELECT m.cluster_id, p.address, p.city, p.state, m.provider_key, p.name
        FROM cluster_members m
        JOIN providers p ON p.provider_key = m.provider_key
        WHERE m.run_id = ?
          AND p.address IS NOT NULL
          AND trim(p.address) <> ''
        """,
        [run_id],
    ).fetchall():
        key = ((address or "").strip(), (city or "").strip(), (state or "").strip())
        slot = addresses.setdefault(cluster_id, {}).setdefault(
            key,
            {"address": key[0], "city": key[1], "state": key[2], "providers": {}},
        )
        slot["providers"][provider_key] = (provider_name or "").strip()

    result: dict[int, dict] = {}
    cluster_ids = set(owners) | set(addresses)
    for cluster_id in cluster_ids:
        owner_rows = []
        for slot in owners.get(cluster_id, {}).values():
            if len(slot["providers"]) < 2:
                continue
            providers = [
                {"key": key, "name": name}
                for key, name in sorted(slot["providers"].items(), key=lambda item: (item[1] or item[0], item[0]))
            ]
            owner_rows.append(
                {
                    "key": slot["key"],
                    "name": slot["name"] or slot["key"],
                    "provider_count": len(providers),
                    "who": _who(providers),
                }
            )
        owner_rows.sort(key=lambda row: (-row["provider_count"], row["name"]))
        address_rows = []
        for slot in addresses.get(cluster_id, {}).values():
            if len(slot["providers"]) < 2:
                continue
            providers = [
                {"key": key, "name": name}
                for key, name in sorted(slot["providers"].items(), key=lambda item: (item[1] or item[0], item[0]))
            ]
            address_rows.append(
                {
                    "place": _place(slot["address"], slot["city"], slot["state"]),
                    "provider_count": len(providers),
                    "who": _who(providers),
                }
            )
        address_rows.sort(key=lambda row: (-row["provider_count"], row["place"]))
        result[cluster_id] = {"owners": owner_rows, "addresses": address_rows}
    return result


def summarize(owners: list[dict], addresses: list[dict], owner_limit: int = 3, address_limit: int = 2) -> tuple[list[str], int]:
    lines = [
        f"{row['name']} · {row['provider_count']} providers"
        for row in owners[:owner_limit]
    ]
    lines.extend(
        f"{row['place']} · {row['provider_count']} providers"
        for row in addresses[:address_limit]
    )
    hidden = max(len(owners) - owner_limit, 0) + max(len(addresses) - address_limit, 0)
    return lines, hidden
