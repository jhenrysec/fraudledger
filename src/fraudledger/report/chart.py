"""Monthly paid chart with a peer-median line.

The vertical scale starts at zero. A line scaled to its own minimum makes an ordinary
month-to-month change look like a surge.
"""

from __future__ import annotations

import math
from datetime import date, datetime
from xml.sax.saxutils import escape

import duckdb

from fraudledger.ingest.focus import HOME_HEALTH_HCPCS, HOSPICE_HCPCS, PERSONAL_CARE_HCPCS
from fraudledger.settings import Settings

_FAMILIES = (
    ("hospice", HOSPICE_HCPCS),
    ("home health", HOME_HEALTH_HCPCS),
    ("personal care", PERSONAL_CARE_HCPCS),
)


def _code_list(codes: frozenset[str]) -> str:
    quoted = []
    for code in sorted(codes):
        if not code.isalnum():
            raise RuntimeError(f"Procedure code is not safe to embed: {code}")
        quoted.append("'" + code + "'")
    return ", ".join(quoted)


def _family_case() -> str:
    branches = []
    for name, codes in _FAMILIES:
        branches.append(
            f"WHEN upper(trim(f.hcpcs)) IN ({_code_list(codes)}) THEN '{name}'"
        )
    return "CASE " + " ".join(branches) + " ELSE 'other' END"


def _facts_token(con: duckdb.DuckDBPyConnection) -> str:
    row = con.execute(
        """
        SELECT COALESCE(string_agg(source_id || ':' || sha256, ',' ORDER BY source_id), '')
        FROM (
            SELECT source_id, arg_max(sha256, retrieved_at) AS sha256
            FROM snapshots
            WHERE source_id IN (
                'medicaid', 'nppes', 'hospice-enrollments', 'home-health-enrollments'
            )
            GROUP BY 1
        )
        """
    ).fetchone()
    return row[0] if row and row[0] else ""


def ensure_peers(con: duckdb.DuckDBPyConnection) -> None:
    """One peer table for every state, code family, and month. Rebuilt when a source changes."""
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS chart_peer (
            family VARCHAR,
            state VARCHAR,
            month DATE,
            median_paid DOUBLE,
            n INTEGER
        )
        """
    )
    con.execute("CREATE TABLE IF NOT EXISTS chart_peer_meta (token VARCHAR)")
    token = _facts_token(con)
    current = con.execute("SELECT token FROM chart_peer_meta LIMIT 1").fetchone()
    if current and current[0] == token and token:
        return
    family_case = _family_case()
    con.execute("DELETE FROM chart_peer")
    con.execute("DELETE FROM chart_peer_meta")
    con.execute(
        f"""
        INSERT INTO chart_peer
        WITH rows AS (
            SELECT
                f.billing_npi,
                CAST(f.month AS DATE) AS month,
                COALESCE(NULLIF(upper(trim(p.state)), ''), '') AS state,
                {family_case} AS family,
                SUM(f.paid) AS paid
            FROM facts_medicaid f
            LEFT JOIN providers p ON p.provider_key = f.billing_npi
            WHERE f.billing_npi IS NOT NULL AND trim(f.billing_npi) <> ''
            GROUP BY 1, 2, 3, 4
        ),
        totals AS (
            SELECT billing_npi, month, state, 'all' AS family, SUM(paid) AS paid
            FROM rows
            GROUP BY 1, 2, 3
        ),
        combined AS (
            SELECT * FROM rows
            UNION ALL
            SELECT * FROM totals
        ),
        by_state AS (
            SELECT family, state, month, median(paid) AS median_paid, COUNT(*)::INTEGER AS n
            FROM combined
            WHERE state <> ''
            GROUP BY 1, 2, 3
        ),
        national AS (
            SELECT family, '' AS state, month, median(paid) AS median_paid, COUNT(*)::INTEGER AS n
            FROM combined
            GROUP BY 1, 3
        )
        SELECT * FROM by_state
        UNION ALL
        SELECT * FROM national
        """
    )
    con.execute("INSERT INTO chart_peer_meta VALUES (?)", [token])


def family_for(codes: set[str]) -> str:
    upper = {code.upper() for code in codes if code}
    for name, known in _FAMILIES:
        if upper and upper <= known:
            return name
    return "all"


def _as_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value)[:10]
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _month_label(value: object) -> str:
    found = _as_date(value)
    if found is None:
        return str(value)[:7]
    return f"{found.year:04d}-{found.month:02d}"


def _money(value: float) -> str:
    number = abs(float(value))
    sign = "-" if value < 0 else ""
    if number >= 1_000_000:
        shown = f"{number / 1_000_000:.1f}"
        if shown.endswith(".0"):
            shown = shown[:-2]
        return f"{sign}${shown}M"
    if number >= 10_000:
        return f"{sign}${number / 1000:.0f}k"
    if number >= 1000:
        shown = f"{number / 1000:.1f}"
        if shown.endswith(".0"):
            shown = shown[:-2]
        return f"{sign}${shown}k"
    return f"{sign}${number:.0f}"


def _ceiling(value: float) -> float:
    if value <= 0:
        return 1.0
    power = 10 ** math.floor(math.log10(value))
    for step in (1, 2, 5, 10):
        ceiling = step * power
        if ceiling >= value * 0.999:
            return float(ceiling)
    return float(10 * power)


def _load_peer_map(
    con: duckdb.DuckDBPyConnection, family: str, state: str
) -> dict[tuple[str, date], tuple[float, int]]:
    rows = con.execute(
        """
        SELECT state, month, median_paid, n
        FROM chart_peer
        WHERE family = ? AND state IN (?, '')
        """,
        [family, state],
    ).fetchall()
    found: dict[tuple[str, date], tuple[float, int]] = {}
    for row_state, month, median_paid, count in rows:
        key = _as_date(month)
        if key is None or median_paid is None:
            continue
        found[(row_state or "", key)] = (float(median_paid), int(count or 0))
    return found


def chart_for(
    con: duckdb.DuckDBPyConnection,
    settings: Settings,
    provider_key: str,
    state: str | None,
    monthly: list[tuple],
) -> dict:
    """Build the labeled chart for one provider.

    monthly rows are (month, paid, comma-separated codes).
    """
    ensure_peers(con)
    points: list[tuple[str, float]] = []
    months: list[date | None] = []
    codes: set[str] = set()
    for row in monthly:
        month, paid = row[0], row[1]
        code_list = row[2] if len(row) > 2 else ""
        points.append((_month_label(month), float(paid or 0)))
        months.append(_as_date(month))
        if code_list:
            codes.update(part for part in str(code_list).split(",") if part)
    if not points:
        return {
            "svg": "",
            "note": "No monthly paid amounts for this provider in the extract.",
        }
    family = family_for(codes)
    state_key = (state or "").upper().strip()
    peers = _load_peer_map(con, family, state_key)
    minimum = int(settings.min_peer_n)
    benchmark: list[float | None] = []
    scopes: list[str] = []
    for month in months:
        if month is None:
            benchmark.append(None)
            scopes.append("none")
            continue
        state_row = peers.get((state_key, month)) if state_key else None
        national_row = peers.get(("", month))
        if state_row and state_row[1] >= minimum:
            benchmark.append(state_row[0])
            scopes.append("state")
        elif national_row and national_row[1] >= minimum:
            benchmark.append(national_row[0])
            scopes.append("national")
        else:
            benchmark.append(None)
            scopes.append("none")
    family_label = "billers in this extract" if family == "all" else f"{family} billers"
    used = {scope for scope in scopes if scope != "none"}
    if not used:
        peer_label = "Peer median"
        note = (
            "Medicaid dollars paid each month. The scale starts at $0. "
            f"No peer median: a month is drawn only when at least {minimum} {family_label} have a payment."
        )
    else:
        where = []
        if "state" in used and state_key:
            where.append(f"{state_key} when that month has at least {minimum} {family_label}")
        if "national" in used:
            where.append(f"the national median of {family_label} otherwise")
        if "state" in used and "national" not in used:
            peer_label = f"{state_key} median"
        elif "national" in used and "state" not in used:
            peer_label = "National median"
        else:
            peer_label = "Peer median"
        joined = ", and ".join(where) if where else "the peer median"
        last_paid = points[-1][1]
        last_peer = next((value for value in reversed(benchmark) if value is not None), None)
        latest = f" Latest month {points[-1][0]}: this provider {_money(last_paid)}"
        if last_peer is not None:
            latest += f", peer median {_money(last_peer)}."
        else:
            latest += "."
        note = (
            "Medicaid dollars paid each month. The scale starts at $0. "
            f"The dashed line is the median paid that month by other {family_label}. "
            f"The peer group is {joined}. "
            "The median is a typical peer, not a loss amount."
            + latest
        )
    return {
        "svg": render_paid_chart(points, benchmark, peer_label),
        "note": note,
    }


def render_paid_chart(
    points: list[tuple[str, float]],
    benchmark: list[float | None] | None = None,
    peer_label: str = "Peer median",
) -> str:
    if not points:
        return ""
    benchmark = list(benchmark or [])
    if len(benchmark) < len(points):
        benchmark.extend([None] * (len(points) - len(benchmark)))
    width, height = 640, 248
    left, right, top, bottom = 64, 16, 36, 36
    values = [value for _, value in points]
    values.extend(value for value in benchmark if value is not None)
    high = _ceiling(max(values) if values else 1)
    plot_w = width - left - right
    plot_h = height - top - bottom

    def xy(index: int, value: float) -> tuple[float, float]:
        x = left + plot_w * (index / max(len(points) - 1, 1))
        y = top + plot_h * (1 - (value / high))
        return x, y

    def polyline(series: list[float | None], stroke: str, dash: str) -> str:
        segments: list[str] = []
        current: list[str] = []
        for index, value in enumerate(series):
            if value is None:
                if len(current) >= 2:
                    segments.append(" ".join(current))
                elif len(current) == 1:
                    segments.append(current[0] + " " + current[0])
                current = []
                continue
            x, y = xy(index, value)
            current.append(f"{x:.1f},{y:.1f}")
        if len(current) >= 2:
            segments.append(" ".join(current))
        elif len(current) == 1:
            segments.append(current[0] + " " + current[0])
        dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
        return "".join(
            f'<polyline fill="none" stroke="{stroke}" stroke-width="2.4"{dash_attr} points="{segment}"/>'
            for segment in segments
        )

    ticks = [0.0, high / 2, high]
    grid = []
    labels = []
    for tick in ticks:
        _, y = xy(0, tick)
        grid.append(
            f'<line x1="{left}" y1="{y:.1f}" x2="{width - right}" y2="{y:.1f}" stroke="#e2e8f0" stroke-width="1"/>'
        )
        labels.append(
            f'<text x="{left - 8}" y="{y + 4:.1f}" text-anchor="end" font-size="12" fill="#5c6b7a">{escape(_money(tick))}</text>'
        )
    label_at = {0, len(points) - 1}
    if len(points) >= 5:
        label_at.add(len(points) // 2)
    xlabels = []
    for index in sorted(label_at):
        x, _ = xy(index, 0)
        anchor = "start" if index == 0 else "end" if index == len(points) - 1 else "middle"
        xlabels.append(
            f'<text x="{x:.1f}" y="{height - 8}" text-anchor="{anchor}" font-size="12" fill="#5c6b7a">{escape(points[index][0])}</text>'
        )
    provider = polyline([value for _, value in points], "#0d9488", "")
    peer = polyline(benchmark[: len(points)], "#64748b", "6 4")
    title = "Medicaid paid by month"
    legend_x = left + 220
    legend = (
        f'<line x1="{legend_x}" y1="16" x2="{legend_x + 22}" y2="16" stroke="#0d9488" stroke-width="2.4"/>'
        f'<text x="{legend_x + 28}" y="20" font-size="12" fill="#142033">This provider</text>'
        f'<line x1="{legend_x + 130}" y1="16" x2="{legend_x + 152}" y2="16" stroke="#64748b" stroke-width="2.4" stroke-dasharray="6 4"/>'
        f'<text x="{legend_x + 158}" y="20" font-size="12" fill="#142033">{escape(peer_label)}</text>'
    )
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
        f'role="img" aria-label="{escape(title)}. Solid line is this provider. Dashed line is the {escape(peer_label)}.">'
        f'<text x="{left}" y="20" font-size="13" font-weight="650" fill="#142033">{escape(title)}</text>'
        f"{legend}"
        f'{"".join(grid)}{"".join(labels)}{"".join(xlabels)}{peer}{provider}'
        f"</svg>"
    )
