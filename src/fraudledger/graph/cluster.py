from __future__ import annotations

import math

import duckdb
import networkx as nx
from networkx.algorithms.community import louvain_communities

from fraudledger.settings import Settings


def build_communities(con: duckdb.DuckDBPyConnection, settings: Settings, run_id: str) -> int:
    con.execute("DELETE FROM clusters WHERE run_id = ?", [run_id])
    con.execute("DELETE FROM cluster_members WHERE run_id = ?", [run_id])
    graph = nx.Graph()
    for owner_key, provider_key, edge_weight in con.execute(
        "SELECT owner_key, provider_key, edge_weight FROM ownership_edges"
    ).fetchall():
        graph.add_edge(
            f"owner:{owner_key}",
            f"provider:{provider_key}",
            weight=float(edge_weight or 0) or 0.1,
        )

    address_rows = con.execute(
        """
        WITH grouped AS (
            SELECT provider_key, address, city, state,
                   COUNT(*) OVER (PARTITION BY address, city, state) AS n
            FROM providers
            WHERE address IS NOT NULL AND address <> ''
        )
        SELECT a.provider_key, b.provider_key
        FROM grouped a
        JOIN grouped b
          ON a.address = b.address
         AND a.city = b.city
         AND a.state = b.state
         AND a.provider_key < b.provider_key
        WHERE a.n >= 3
        """
    ).fetchall()
    for left, right in address_rows:
        graph.add_edge(f"provider:{left}", f"provider:{right}", weight=1.2)

    if graph.number_of_edges() == 0:
        return 0

    communities = louvain_communities(
        graph,
        weight="weight",
        resolution=settings.louvain_resolution,
        seed=settings.louvain_seed,
    )
    flagged = {
        row[0]
        for row in con.execute(
            "SELECT DISTINCT provider_key FROM flags WHERE run_id = ?",
            [run_id],
        ).fetchall()
    }
    excess = {
        row[0]: float(row[1] or 0)
        for row in con.execute(
            """
            SELECT provider_key, SUM(excess_dollars)
            FROM flags
            WHERE run_id = ? AND excess_dollars IS NOT NULL
            GROUP BY 1
            """,
            [run_id],
        ).fetchall()
    }
    r11 = {
        row[0]
        for row in con.execute(
            "SELECT DISTINCT provider_key FROM flags WHERE run_id = ? AND rule_id = 'R11'",
            [run_id],
        ).fetchall()
    }

    written = 0
    cluster_id = 0
    for community in communities:
        providers = sorted(node.split(":", 1)[1] for node in community if node.startswith("provider:"))
        owners = sorted(node.split(":", 1)[1] for node in community if node.startswith("owner:"))
        if len(providers) < 2:
            continue
        flagged_count = sum(1 for key in providers if key in flagged)
        dollars = sum(excess.get(key, 0) for key in providers)
        any_r11 = any(key in r11 for key in providers)
        size_term = min(len(providers), 10) * 0.5
        score = flagged_count * 2 + math.log1p(max(dollars, 0)) + size_term + (3 if any_r11 else 0)
        con.execute(
            """
            INSERT INTO clusters (
                run_id, cluster_id, provider_count, owner_count, flagged_count, score, excess_dollars
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [run_id, cluster_id, len(providers), len(owners), flagged_count, score, dollars],
        )
        con.executemany(
            "INSERT INTO cluster_members (run_id, cluster_id, provider_key) VALUES (?, ?, ?)",
            [(run_id, cluster_id, key) for key in providers],
        )
        written += 1
        cluster_id += 1
    return written
