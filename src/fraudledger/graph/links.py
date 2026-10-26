"""Owners and street addresses that connect more than one provider in a group."""

from __future__ import annotations

import duckdb


def _who(providers: list[dict], limit: int = 8) -> str:
    names = [item["name"] or item["key"] for item in providers]
    if len(names) <= limit:
        return ", ".join(names)
    rest = len(names) - limit
    return ", ".join(names[:limit]) + f", and {rest} more"


def _is_po_box(address: str) -> bool:
    text = (address or "").upper()
    return "PO BOX" in text or "P.O. BOX" in text or "POST OFFICE BOX" in text


def _provider_bit(name: str, key: str) -> str:
    who = (name or "").strip() or "Name not in the directory"
    return f"{who} (NPI {key})"


def _join_bits(bits: list[str], more: int) -> str:
    if not bits:
        return ""
    text = ", ".join(bits)
    if more > 0:
        text += f", and {more} more"
    return text


def explain_membership(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    provider_key: str,
    address: str | None,
    city: str | None,
    state: str | None,
    owner_rows: list[tuple],
    address_link_max: int,
) -> list[str]:
    """Say exactly why this provider is in a group, naming owners and addresses.

    A reader should not have to guess whether the link is an owner, a street
    address, or neither. owner_rows are (owner_key, owner_name, role_code, percent)
    for this provider only.
    """
    from fraudledger.reference import role_label

    groups = [
        int(row[0])
        for row in con.execute(
            """
            SELECT cluster_id FROM cluster_members
            WHERE run_id = ? AND provider_key = ?
            ORDER BY 1
            """,
            [run_id, provider_key],
        ).fetchall()
    ]
    if not groups:
        return [
            "This provider is not in an ownership group in this run. "
            "A group is created only when providers share a PECOS associate id, "
            "or when 3 to "
            f"{int(address_link_max)} providers use the same street address. "
            "A post-office box is not used as a link."
        ]

    roles: dict[str, list[str]] = {}
    names: dict[str, str] = {}
    for owner_key, owner_name, role, _percent in owner_rows:
        key = owner_key or "unknown"
        roles.setdefault(key, []).append(role_label(role))
        shown = (owner_name or "").strip()
        if len(shown) > len(names.get(key, "")):
            names[key] = shown

    lines: list[str] = []
    street = (address or "").strip()
    place = ", ".join(part for part in (street, (city or "").strip(), (state or "").strip()) if part)
    for cluster_id in groups:
        lines.append(
            f"Group {cluster_id} is a label for one set of linked providers. "
            "It is not a rank and not a severity score."
        )
        if not street:
            lines.append(
                "No street address was loaded for this provider, so an address cannot be why it is in the group."
            )
            same_address: list[tuple] = []
            directory_count = 0
        elif _is_po_box(street):
            lines.append(
                f"Directory address: {place}. "
                "That is a post-office box. A post-office box is not used as a group link."
            )
            same_address = []
            directory_count = 0
        else:
            same_address = con.execute(
                """
                SELECT p.provider_key, COALESCE(p.name, '')
                FROM cluster_members m
                JOIN providers p ON p.provider_key = m.provider_key
                WHERE m.run_id = ?
                  AND m.cluster_id = ?
                  AND m.provider_key <> ?
                  AND upper(trim(p.address)) = upper(trim(?))
                  AND upper(trim(coalesce(p.city, ''))) = upper(trim(?))
                  AND upper(trim(coalesce(p.state, ''))) = upper(trim(?))
                ORDER BY p.name, p.provider_key
                """,
                [run_id, cluster_id, provider_key, street, city or "", state or ""],
            ).fetchall()
            directory_count = int(
                con.execute(
                    """
                    SELECT COUNT(*)
                    FROM providers
                    WHERE upper(trim(address)) = upper(trim(?))
                      AND upper(trim(coalesce(city, ''))) = upper(trim(?))
                      AND upper(trim(coalesce(state, ''))) = upper(trim(?))
                    """,
                    [street, city or "", state or ""],
                ).fetchone()[0]
            )
            if same_address:
                named = [_provider_bit(name, key) for key, name in same_address[:8]]
                more = max(len(same_address) - len(named), 0)
                qualified = 3 <= directory_count <= int(address_link_max)
                reason = (
                    f"{directory_count} providers in the directory use that street, city, and state. "
                    f"An address is used as a group link when that count is at least 3 and at most {int(address_link_max)}."
                )
                if qualified:
                    reason += " This address meets that rule, so it is a link."
                else:
                    reason += " This address does not meet that rule, so it was not used as a link."
                lines.append(
                    f"Directory address: {place}. "
                    f"Other providers in this group at that address: {_join_bits(named, more)}. "
                    + reason
                )
            else:
                verb = "uses" if directory_count == 1 else "use"
                lines.append(
                    f"Directory address: {place}. "
                    "No other provider in this group uses that street address. "
                    f"{directory_count} provider{'s' if directory_count != 1 else ''} in the directory "
                    f"{verb} that street, city, and state. "
                    "A shared street address is not why this provider is in the group. "
                    f"An address is used as a link only when 3 to {int(address_link_max)} providers use it, "
                    "and a post-office box is never used."
                )

        shared = con.execute(
            """
            SELECT e.owner_key,
                   o.provider_key,
                   COALESCE(p.name, ''),
                   MAX(o.owner_name)
            FROM ownership_edges e
            JOIN ownership_edges o
              ON o.owner_key = e.owner_key
             AND o.provider_key <> e.provider_key
            JOIN cluster_members m
              ON m.provider_key = o.provider_key
             AND m.run_id = ?
             AND m.cluster_id = ?
            LEFT JOIN providers p ON p.provider_key = o.provider_key
            WHERE e.provider_key = ?
            GROUP BY e.owner_key, o.provider_key, p.name
            ORDER BY e.owner_key, p.name, o.provider_key
            """,
            [run_id, cluster_id, provider_key],
        ).fetchall()
        by_owner: dict[str, dict] = {}
        for owner_key, other_key, other_name, owner_name in shared:
            slot = by_owner.setdefault(
                owner_key,
                {"name": (owner_name or "").strip(), "providers": []},
            )
            if len((owner_name or "").strip()) > len(slot["name"]):
                slot["name"] = (owner_name or "").strip()
            slot["providers"].append((other_key, other_name))
        if by_owner:
            lines.append(
                "The PECOS associate ids below are on this provider and on at least one other provider in the group. "
                "That shared id is the link. The id is not an NPI."
            )
            shown_owners = list(by_owner.items())[:12]
            for owner_key, slot in shown_owners:
                who = names.get(owner_key) or slot["name"] or "Unnamed owner"
                relationship = roles.get(owner_key) or ["relationship not recorded"]
                # one label per distinct relationship, keep order
                unique = list(dict.fromkeys(relationship))
                others = []
                seen = set()
                for other_key, other_name in slot["providers"]:
                    if other_key in seen:
                        continue
                    seen.add(other_key)
                    others.append(_provider_bit(other_name, other_key))
                named = others[:8]
                more = max(len(others) - len(named), 0)
                lines.append(
                    f"{who} (PECOS associate id {owner_key}) is on this provider as "
                    + " and ".join(unique)
                    + f". The same associate id is also on {_join_bits(named, more)}."
                )
            hidden_owners = len(by_owner) - len(shown_owners)
            if hidden_owners:
                lines.append(f"{hidden_owners} more shared associate ids are on this provider and are not listed here.")
        else:
            lines.append(
                "None of the PECOS associate ids on this provider are on another provider in this group. "
                "A shared owner is not the direct link."
            )
        if not by_owner and not same_address:
            others = con.execute(
                """
                SELECT e.owner_key, MAX(e.owner_name), COUNT(DISTINCT e.provider_key)
                FROM ownership_edges e
                JOIN cluster_members m
                  ON m.provider_key = e.provider_key
                 AND m.run_id = ?
                 AND m.cluster_id = ?
                WHERE e.provider_key <> ?
                GROUP BY e.owner_key
                HAVING COUNT(DISTINCT e.provider_key) >= 2
                ORDER BY 3 DESC, 1
                LIMIT 5
                """,
                [run_id, cluster_id, provider_key],
            ).fetchall()
            if others:
                bits = [
                    f"{(name or 'Unnamed').strip()} (PECOS associate id {key}) is on {int(count)} other providers"
                    for key, name, count in others
                ]
                lines.append(
                    "This provider is still in the group because other members are linked to each other. "
                    "These links are not on this provider: "
                    + "; ".join(bits)
                    + "."
                )
    return lines


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
