from __future__ import annotations

import json
import re
import threading
import traceback
from contextvars import ContextVar
from html import escape
from pathlib import Path

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from fraudledger import __version__
from fraudledger.db import connect, refresh_facts_view
from fraudledger.detect.engine import RULE_BLURB
from fraudledger.detect.score import gap_by_provider, recount_cluster, unduplicated_excess
from fraudledger.graph.links import explain_membership, shared_links, summarize
from fraudledger.ingest.cleanup import purge_reserved
from fraudledger.reference import explain_cluster, origin_label, reader_index, role_label
from fraudledger.report.assemble import DISCLAIMER
from fraudledger.report.chart import chart_for
from fraudledger.report.packet import _explain_flag, _heading_for, _weight_sentence
from fraudledger.reviews import ALLOWED_STATES, PROVIDER_KEY, set_review
from fraudledger.settings import Settings, load_settings

RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")
_LOCK = threading.Lock()
_REQUEST: ContextVar[Request | None] = ContextVar("fraudledger_request", default=None)


def _shown_group(header, members: dict, gaps: dict) -> dict:
    score, dollars = recount_cluster(header[4], header[5], members.get(header[0], []), gaps)
    return explain_cluster(header[1], header[2], header[3], score, dollars)


def _percent(value: object) -> int | None:
    if value is None or value == "":
        return None
    try:
        number = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return max(0, min(100, number))


def _bytes_label(value: object) -> str:
    try:
        amount = int(value or 0)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return ""
    if amount <= 0:
        return ""
    if amount >= 1_000_000_000:
        return f"{amount / 1_000_000_000:.1f} GB"
    if amount >= 1_000_000:
        return f"{amount / 1_000_000:.1f} MB"
    return f"{max(amount / 1_000, 1):.0f} KB"


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or load_settings()
    settings.ensure_dirs()
    connection = connect(settings.db_path)
    try:
        purge_reserved(connection, settings)
    finally:
        connection.close()
    templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent.parent / "templates"))
    templates.env.filters["role_label"] = role_label
    templates.env.filters["origin_label"] = origin_label
    app = FastAPI(title="Fraud Ledger", version=__version__, docs_url=None, redoc_url=None)

    @app.middleware("http")
    async def remember_request(request: Request, call_next):
        token = _REQUEST.set(request)
        try:
            return await call_next(request)
        finally:
            _REQUEST.reset(token)

    def db():
        connection = connect(settings.db_path)
        refresh_facts_view(connection, settings.data_dir / "parquet" / "facts")
        return connection

    def call(fn):
        request = _REQUEST.get()
        if not _LOCK.acquire(timeout=1):
            if request is not None and request.method == "GET":
                return loading_page(request)
            raise HTTPException(
                409,
                "A file is being written into the database. Try again when that write finishes.",
            )
        try:
            connection = db()
            try:
                return fn(connection)
            finally:
                connection.close()
        finally:
            _LOCK.release()

    def latest(connection):
        return connection.execute(
            """
            SELECT run_id, demo_only, started_at, finished_at, status, config_sha256
            FROM runs
            WHERE status = 'succeeded'
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()

    def resolve_run(connection, requested: str | None):
        if requested:
            if not RUN_ID.match(requested):
                raise HTTPException(400, "Unrecognized run id")
            row = connection.execute(
                """
                SELECT run_id, demo_only, started_at, finished_at, status, config_sha256
                FROM runs WHERE run_id = ?
                """,
                [requested],
            ).fetchone()
            if row is None:
                raise HTTPException(404, "Run not found")
            return row
        row = latest(connection)
        if row is None:
            raise HTTPException(404, "No completed run yet")
        return row

    def shell(request: Request, connection, run_row, **extra):
        demo = bool(run_row[1]) if run_row else False
        if "progress" in extra:
            progress = extra.pop("progress")
        else:
            status = read_status()
            progress = status if status.get("running") else None
        return {
            "request": request,
            "version": __version__,
            "disclaimer": DISCLAIMER,
            "demo": demo,
            "run_id": run_row[0] if run_row else None,
            "progress": progress,
            **extra,
        }

    @app.get("/health")
    def health():
        return {"status": "ok", "version": __version__}

    @app.get("/", response_class=HTMLResponse)
    def queue(request: Request, run: str | None = None):
        def read(connection):
            try:
                run_row = resolve_run(connection, run)
            except HTTPException as exc:
                if exc.status_code == 404 and run is None:
                    return templates.TemplateResponse(
                        request,
                        "queue.html",
                        shell(request, connection, None, rows=[], empty=True),
                    )
                raise
            rows = connection.execute(
                """
                SELECT s.provider_key, COALESCE(p.name, s.provider_key), p.city, p.state,
                       s.score, s.tier, s.rules, s.excess_dollars, s.flag_count,
                       COALESCE(r.state, 'unreviewed')
                FROM scores s
                LEFT JOIN providers p ON p.provider_key = s.provider_key
                LEFT JOIN review_states r ON r.provider_key = s.provider_key
                WHERE s.run_id = ?
                ORDER BY s.score DESC, s.provider_key
                """,
                [run_row[0]],
            ).fetchall()
            gaps = gap_by_provider(connection, run_row[0])
            rows = [(*row[:7], gaps.get(row[0], 0.0), row[8], row[9]) for row in rows]
            return templates.TemplateResponse(
                request,
                "queue.html",
                shell(request, connection, run_row, rows=rows, empty=False),
            )

        return call(read)

    @app.get("/provider/{provider_key}", response_class=HTMLResponse)
    def provider(request: Request, provider_key: str, run: str | None = None):
        if not PROVIDER_KEY.match(provider_key):
            raise HTTPException(400, "Unrecognized provider key")

        def read(connection):
            run_row = resolve_run(connection, run)
            identity = connection.execute(
                """
                SELECT name, entity_type, taxonomy, address, city, state, zip,
                       enumeration_date, incorporation_date, origin
                FROM providers WHERE provider_key = ?
                """,
                [provider_key],
            ).fetchone()
            score = connection.execute(
                """
                SELECT score, tier, rules, excess_dollars, flag_count
                FROM scores WHERE run_id = ? AND provider_key = ?
                """,
                [run_row[0], provider_key],
            ).fetchone()
            flags = connection.execute(
                """
                SELECT rule_id, metric_value, threshold, peer_value, excess_dollars, detail_json
                FROM flags
                WHERE run_id = ? AND provider_key = ?
                ORDER BY rule_id
                """,
                [run_row[0], provider_key],
            ).fetchall()
            indicators = [
                {
                    "heading": _heading_for(rule_id, detail),
                    "text": _explain_flag(rule_id, metric, threshold, peer, dollars, detail),
                }
                for rule_id, metric, threshold, peer, dollars, detail in flags
            ]
            rule_ids = [part for part in str(score[2]).split(",") if part] if score and score[2] else []
            weight_sentence = _weight_sentence(settings, rule_ids, float(score[0])) if score else ""
            gap = unduplicated_excess(
                (rule_id, dollars) for rule_id, _metric, _threshold, _peer, dollars, _detail in flags
            )
            monthly = connection.execute(
                """
                SELECT month, SUM(paid), string_agg(DISTINCT upper(trim(hcpcs)), ',')
                FROM facts_medicaid
                WHERE billing_npi = ?
                GROUP BY 1
                ORDER BY 1
                """,
                [provider_key],
            ).fetchall()
            chart = chart_for(
                connection,
                settings,
                provider_key,
                identity[5] if identity else None,
                monthly,
            )
            edges = connection.execute(
                """
                SELECT owner_key, owner_name, percentage, role_code, edge_weight, origin
                FROM ownership_edges
                WHERE provider_key = ?
                ORDER BY edge_weight DESC, owner_key
                """,
                [provider_key],
            ).fetchall()
            link_lines = explain_membership(
                connection,
                run_row[0],
                provider_key,
                identity[3] if identity else None,
                identity[4] if identity else None,
                identity[5] if identity else None,
                [(owner, name, role, percent) for owner, name, percent, role, _weight, _origin in edges],
                settings.address_link_max,
            )
            clusters = connection.execute(
                """
                SELECT cluster_id FROM cluster_members
                WHERE run_id = ? AND provider_key = ?
                ORDER BY cluster_id
                """,
                [run_row[0], provider_key],
            ).fetchall()
            review = connection.execute(
                "SELECT state, note, updated_at FROM review_states WHERE provider_key = ?",
                [provider_key],
            ).fetchone()
            return templates.TemplateResponse(
                request,
                "provider.html",
                shell(
                    request,
                    connection,
                    run_row,
                    provider_key=provider_key,
                    identity=identity,
                    score=score,
                    weight_sentence=weight_sentence,
                    gap=gap,
                    indicators=indicators,
                    spark=chart["svg"],
                    chart_note=chart["note"],
                    edges=edges,
                    link_lines=link_lines,
                    clusters=[row[0] for row in clusters],
                    review=review,
                    states=ALLOWED_STATES,
                ),
            )

        return call(read)

    @app.get("/clusters", response_class=HTMLResponse)
    def clusters(request: Request, run: str | None = None):
        def read(connection):
            run_row = resolve_run(connection, run)
            rows = connection.execute(
                """
                SELECT cluster_id, provider_count, owner_count, flagged_count, score, excess_dollars
                FROM clusters
                WHERE run_id = ?
                ORDER BY score DESC, cluster_id
                """,
                [run_row[0]],
            ).fetchall()
            gaps = gap_by_provider(connection, run_row[0])
            member_rows = connection.execute(
                "SELECT cluster_id, provider_key FROM cluster_members WHERE run_id = ?",
                [run_row[0]],
            ).fetchall()
            members: dict[int, list[str]] = {}
            for cluster_id, key in member_rows:
                members.setdefault(cluster_id, []).append(key)
            links = shared_links(connection, run_row[0])
            groups = []
            for row in rows:
                shared = links.get(row[0], {"owners": [], "addresses": []})
                brief, more = summarize(shared["owners"], shared["addresses"])
                groups.append(
                    {
                        "id": row[0],
                        **_shown_group(row, members, gaps),
                        "brief": brief,
                        "more": more,
                    }
                )
            return templates.TemplateResponse(
                request,
                "clusters.html",
                shell(request, connection, run_row, groups=groups),
            )

        return call(read)

    @app.get("/cluster/{cluster_id}", response_class=HTMLResponse)
    def cluster(request: Request, cluster_id: int, run: str | None = None):
        def read(connection):
            run_row = resolve_run(connection, run)
            header = connection.execute(
                """
                SELECT cluster_id, provider_count, owner_count, flagged_count, score, excess_dollars
                FROM clusters WHERE run_id = ? AND cluster_id = ?
                """,
                [run_row[0], cluster_id],
            ).fetchone()
            if header is None:
                raise HTTPException(404, "Cluster not found in this run")
            members = connection.execute(
                """
                SELECT m.provider_key, p.name, p.city, p.state,
                       s.tier, s.score, s.rules
                FROM cluster_members m
                LEFT JOIN providers p ON p.provider_key = m.provider_key
                LEFT JOIN scores s ON s.run_id = m.run_id AND s.provider_key = m.provider_key
                WHERE m.run_id = ? AND m.cluster_id = ?
                ORDER BY s.score DESC NULLS LAST, m.provider_key
                """,
                [run_row[0], cluster_id],
            ).fetchall()
            owners = connection.execute(
                """
                SELECT DISTINCT e.owner_key, e.owner_name, e.role_code, e.percentage
                FROM ownership_edges e
                JOIN cluster_members m ON m.provider_key = e.provider_key
                WHERE m.run_id = ? AND m.cluster_id = ?
                ORDER BY e.owner_key
                """,
                [run_row[0], cluster_id],
            ).fetchall()
            flagged_members = [row for row in members if row[5] is not None]
            quiet_members = [row for row in members if row[5] is None]
            gaps = gap_by_provider(connection, run_row[0])
            group = _shown_group(
                header,
                {header[0]: [row[0] for row in members]},
                gaps,
            )
            shared = shared_links(connection, run_row[0]).get(
                header[0],
                {"owners": [], "addresses": []},
            )
            return templates.TemplateResponse(
                request,
                "cluster.html",
                shell(
                    request,
                    connection,
                    run_row,
                    group_id=header[0],
                    group=group,
                    flagged_members=flagged_members,
                    quiet_members=quiet_members,
                    owners=owners,
                    shared_owners=shared["owners"],
                    shared_addresses=shared["addresses"],
                ),
            )

        return call(read)

    @app.get("/rules", response_class=HTMLResponse)
    def rules(request: Request):
        def read(connection):
            run_row = connection.execute(
                """
                SELECT run_id, demo_only FROM runs
                WHERE status = 'succeeded'
                ORDER BY started_at DESC LIMIT 1
                """
            ).fetchone()
            catalog = []
            for rule_id, blurb in RULE_BLURB.items():
                cfg = dict(settings.rules.get(rule_id) or {})
                catalog.append(
                    {
                        "id": rule_id,
                        "title": cfg.get("title") or rule_id,
                        "weight": cfg.get("weight", ""),
                        "blurb": blurb,
                        "settings": {key: value for key, value in cfg.items() if key not in {"title", "weight"}},
                    }
                )
            return templates.TemplateResponse(
                request,
                "rules.html",
                shell(
                    request,
                    connection,
                    run_row,
                    catalog=catalog,
                    config_sha=settings.config_sha256,
                    cluster_bonus_max=settings.cluster_bonus_max_providers,
                ),
            )

        return call(read)

    @app.get("/index", response_class=HTMLResponse)
    def reader(request: Request):
        def read(connection):
            run_row = connection.execute(
                """
                SELECT run_id, demo_only FROM runs
                WHERE status = 'succeeded'
                ORDER BY started_at DESC LIMIT 1
                """
            ).fetchone()
            return templates.TemplateResponse(
                request,
                "index.html",
                shell(request, connection, run_row, index=reader_index(settings)),
            )

        return call(read)

    @app.get("/runs", response_class=HTMLResponse)
    def runs(request: Request):
        def read(connection):
            rows = connection.execute(
                """
                SELECT run_id, started_at, finished_at, status, demo_only, config_sha256, git_sha
                FROM runs
                ORDER BY started_at DESC
                """
            ).fetchall()
            run_row = (rows[0][0], rows[0][4]) if rows else None
            return templates.TemplateResponse(
                request,
                "runs.html",
                shell(request, connection, run_row, rows=rows),
            )

        return call(read)

    @app.get("/runs/{run_id}", response_class=HTMLResponse)
    def run_detail(request: Request, run_id: str):
        def read(connection):
            run_row = resolve_run(connection, run_id)
            artifacts = connection.execute(
                """
                SELECT path, sha256, kind FROM run_artifacts
                WHERE run_id = ?
                ORDER BY path
                """,
                [run_id],
            ).fetchall()
            return templates.TemplateResponse(
                request,
                "run.html",
                shell(request, connection, run_row, artifacts=artifacts),
            )

        return call(read)

    @app.post("/review")
    def review(
        provider_key: str = Form(...),
        state: str = Form(...),
        note: str = Form(""),
        run: str = Form(""),
    ):
        if not PROVIDER_KEY.match(provider_key):
            raise HTTPException(400, "Unrecognized provider key")

        def write(connection):
            try:
                set_review(connection, provider_key, state, note)
            except SystemExit as exc:
                raise HTTPException(400, str(exc)) from exc

        call(write)
        target = f"/provider/{provider_key}"
        if run and RUN_ID.match(run):
            target += f"?run={run}"
        return RedirectResponse(target, status_code=303)

    @app.get("/export/{run_id}")
    def export(run_id: str):
        if not RUN_ID.match(run_id):
            raise HTTPException(400, "Unrecognized run id")
        from fraudledger.cli import _export_zip

        try:
            path = _export_zip(settings, run_id)
        except SystemExit as exc:
            raise HTTPException(404, str(exc)) from exc
        return FileResponse(path, filename=path.name, media_type="application/zip")

    @app.get("/group/{cluster_id}/report")
    def group_report(cluster_id: int, run: str | None = None):
        from fraudledger.report.group import build_group_packet

        def write(connection):
            run_row = resolve_run(connection, run)
            try:
                return build_group_packet(connection, settings, run_row[0], cluster_id)
            except SystemExit as exc:
                text = str(exc)
                lowered = text.lower()
                status = 404 if "no group" in lowered or "no run" in lowered else 400
                raise HTTPException(status, text) from exc

        path = call(write)
        return FileResponse(path, filename=path.name, media_type="application/zip")

    @app.get("/report/{provider_key}")
    def report_packet(provider_key: str, run: str | None = None):
        if not PROVIDER_KEY.match(provider_key):
            raise HTTPException(400, "Unrecognized provider key")
        from fraudledger.report.packet import build_packet

        def write(connection):
            run_row = resolve_run(connection, run)
            try:
                path = build_packet(connection, settings, run_row[0], provider_key)
                set_review(
                    connection,
                    provider_key,
                    "exported",
                    "Report packet saved on this machine. It was not submitted.",
                )
            except SystemExit as exc:
                text = str(exc)
                lowered = text.lower()
                if "not in this run" in lowered or "no run" in lowered:
                    status = 404
                else:
                    status = 400
                raise HTTPException(status, text) from exc
            return path

        path = call(write)
        return FileResponse(path, filename=path.name, media_type="application/zip")

    status_path = settings.data_dir / "logs" / "pull_status.json"

    def read_status() -> dict:
        if not status_path.is_file():
            return {"running": False, "message": "", "error": "", "percent": None, "received_label": ""}
        try:
            data = json.loads(status_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return {"running": False, "message": "", "error": "", "percent": None, "received_label": ""}
        return {
            "running": bool(data.get("running")),
            "message": str(data.get("message") or ""),
            "error": str(data.get("error") or ""),
            "percent": _percent(data.get("percent")),
            "received_label": _bytes_label(data.get("received")),
        }

    def write_status(**fields: object) -> dict:
        current = read_status()
        current.update(fields)
        status_path.parent.mkdir(parents=True, exist_ok=True)
        blob = json.dumps(current)
        temporary = status_path.with_suffix(".tmp")
        temporary.write_text(blob, encoding="utf-8")
        temporary.replace(status_path)
        return current

    def loading_page(request: Request):
        status = read_status()
        progress = status if status.get("running") else {
            "message": "Writing a downloaded file into the database",
            "percent": None,
            "received_label": "",
        }
        return templates.TemplateResponse(
            request,
            "loading.html",
            {
                "request": request,
                "version": __version__,
                "disclaimer": DISCLAIMER,
                "demo": False,
                "run_id": None,
                "progress": progress,
            },
        )

    if read_status().get("running"):
        write_status(running=False, error="The previous pull stopped before it finished.")

    def sources_page(request: Request, connection):
        from fraudledger.ingest.catalog import CUSTOM_KINDS, KNOWN_SOURCES

        status = read_status()
        run_row = None
        if connection is not None and not status["running"]:
            run_row = latest(connection)
        context = shell(
            request,
            connection,
            run_row,
            sources=KNOWN_SOURCES,
            kinds=CUSTOM_KINDS,
            running=status["running"],
            message=status["message"],
            error=status["error"],
            percent=status.get("percent"),
            received_label=status.get("received_label") or "",
            progress=None,
        )
        return templates.TemplateResponse(request, "sources.html", context)

    @app.get("/sources", response_class=HTMLResponse)
    def sources(request: Request):
        status = read_status()
        if status["running"]:
            return sources_page(request, None)

        def read(connection):
            return sources_page(request, connection)

        return call(read)

    @app.post("/sources")
    def start_pull(
        mode: str = Form(...),
        source: str = Form(""),
        url: str = Form(""),
        as_kind: str = Form(""),
        run_after: str = Form(""),
        focus: str = Form(""),
    ):
        from fraudledger.ingest.catalog import CUSTOM_KINDS, KNOWN_SOURCES, validate_http_url
        from fraudledger.pipeline import execute_run

        if read_status().get("running"):
            return RedirectResponse("/sources", status_code=303)
        known = {item.key for item in KNOWN_SOURCES}
        kinds = {item[0] for item in CUSTOM_KINDS}
        score = run_after == "1" or mode == "working"
        chosen_focus = "priority" if focus == "priority" else None
        if mode == "working":
            target_key = "working"
            target_url = None
        elif mode == "url":
            try:
                chosen = validate_http_url(url)
            except SystemExit as exc:
                raise HTTPException(400, str(exc)) from exc
            if as_kind not in kinds:
                raise HTTPException(400, "Choose how to read that URL.")
            target_key = as_kind
            target_url = chosen
        elif mode == "known":
            if source not in known:
                raise HTTPException(400, "Unknown public source.")
            target_key = source
            target_url = None
        else:
            raise HTTPException(400, "Unknown pull mode.")

        write_status(running=True, message=f"Starting {target_key}.", error="", percent=0, received=0, total=0)

        def reporter(message, received, total) -> None:
            from fraudledger.ingest.common import percent_of

            write_status(
                running=True,
                message=str(message or ""),
                error="",
                received=int(received or 0),
                total=int(total or 0),
                percent=percent_of(received, total),
                received_label=_bytes_label(received),
            )

        def job() -> None:
            from fraudledger.ingest.common import download, set_progress_reporter
            from fraudledger.ingest.pull import LABELS, OPTIONAL_KEYS, load_local, plan, plan_working

            connection = None
            set_progress_reporter(reporter)
            try:
                if target_key == "working":
                    steps = plan_working(settings)
                else:
                    steps = plan(
                        settings,
                        target_key,
                        url=target_url,
                        focus=chosen_focus,
                    )
                messages: list[str] = []
                for key, source_url, options in steps:
                    if key == "skip":
                        messages.append(source_url)
                        continue
                    label = LABELS.get(key, key)
                    dest = settings.data_dir / "tmp" / f"{key}.download"
                    try:
                        download(source_url, dest, label=label)
                        with _LOCK:
                            connection = db()
                            try:
                                messages.append(
                                    load_local(
                                        connection,
                                        settings,
                                        key,
                                        dest,
                                        source_url,
                                        focus=options.get("focus"),
                                    )
                                )
                            finally:
                                connection.close()
                                connection = None
                    except SystemExit as exc:
                        if key not in OPTIONAL_KEYS:
                            raise
                        messages.append(f"{label} skipped: {exc}")
                text = " ".join(messages)
                if score:
                    with _LOCK:
                        connection = db()
                        try:
                            facts = connection.execute("SELECT COUNT(*) FROM facts_medicaid").fetchone()[0]
                            if facts:
                                run_id = execute_run(connection, settings)
                                text += f" Run {run_id}."
                            else:
                                text += " No billing facts yet, so detection was not run."
                        finally:
                            connection.close()
                            connection = None
                write_status(running=False, message=text, error="")
            except SystemExit as exc:
                write_status(running=False, message="", error=str(exc))
            except Exception as exc:
                write_status(running=False, message="", error=str(exc))
            finally:
                set_progress_reporter(None)
                if connection is not None:
                    connection.close()

        threading.Thread(target=job, name="fraudledger-pull", daemon=True).start()
        return RedirectResponse("/sources", status_code=303)

    @app.exception_handler(Exception)
    async def unhandled(request: Request, exc: Exception):
        log = settings.data_dir / "logs" / "server.log"
        log.parent.mkdir(parents=True, exist_ok=True)
        with log.open("a", encoding="utf-8") as handle:
            handle.write(traceback.format_exc())
            handle.write("\n")
        if request.url.path == "/health" or "application/json" in request.headers.get("accept", ""):
            return JSONResponse({"detail": f"{type(exc).__name__}: {exc}"}, status_code=500)
        text = escape(f"{type(exc).__name__}: {exc}")
        body = (
            "<!DOCTYPE html><html lang='en'><head><meta charset='utf-8'><title>Fraud Ledger</title></head>"
            "<body style='font-family:sans-serif;background:#f4f6f8;color:#142033;padding:40px;'>"
            "<p><a href='/'>Fraud Ledger</a></p><h1>Something broke while drawing this page</h1>"
            f"<p>{text}</p><p>The same detail is in the server log. The home health rows already loaded are still in the database.</p>"
            "</body></html>"
        )
        return HTMLResponse(body, status_code=500)

    @app.exception_handler(HTTPException)
    async def http_error(request: Request, exc: HTTPException):
        if request.url.path == "/health" or "application/json" in request.headers.get("accept", ""):
            return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)
        detail = escape(str(exc.detail))
        body = (
            "<!DOCTYPE html><html lang='en'><head><meta charset='utf-8'><title>Fraud Ledger</title></head>"
            "<body style='font-family:sans-serif;background:#f4f6f8;color:#142033;padding:40px;'>"
            "<p><a href='/'>Fraud Ledger</a></p>"
            f"<h1>{exc.status_code}</h1><p>{detail}</p></body></html>"
        )
        return HTMLResponse(body, status_code=exc.status_code)

    return app
