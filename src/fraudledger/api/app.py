from __future__ import annotations

import json
import re
import threading
from pathlib import Path

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from fraudledger import __version__
from fraudledger.db import connect, refresh_facts_view
from fraudledger.detect.engine import RULE_BLURB
from fraudledger.report.assemble import DISCLAIMER, _svg
from fraudledger.reviews import ALLOWED_STATES, PROVIDER_KEY, set_review
from fraudledger.settings import Settings, load_settings

RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")
_LOCK = threading.Lock()


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or load_settings()
    settings.ensure_dirs()
    templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent.parent / "templates"))
    app = FastAPI(title="Fraud Ledger", version=__version__, docs_url=None, redoc_url=None)

    def db():
        connection = connect(settings.db_path)
        refresh_facts_view(connection, settings.data_dir / "parquet" / "facts")
        return connection

    def call(fn):
        with _LOCK:
            connection = db()
            try:
                return fn(connection)
            finally:
                connection.close()

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
        return {
            "request": request,
            "version": __version__,
            "disclaimer": DISCLAIMER,
            "demo": demo,
            "run_id": run_row[0] if run_row else None,
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
                SELECT name, entity_type, taxonomy, address, city, state, zip, enumeration_date, origin
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
            monthly = connection.execute(
                """
                SELECT month, SUM(paid)
                FROM facts_medicaid
                WHERE billing_npi = ?
                GROUP BY 1
                ORDER BY 1
                """,
                [provider_key],
            ).fetchall()
            edges = connection.execute(
                """
                SELECT owner_key, owner_name, percentage, role_code, edge_weight, origin
                FROM ownership_edges
                WHERE provider_key = ?
                ORDER BY edge_weight DESC, owner_key
                """,
                [provider_key],
            ).fetchall()
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
                    flags=flags,
                    spark=_svg([(str(month), float(paid or 0)) for month, paid in monthly]),
                    edges=edges,
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
            return templates.TemplateResponse(
                request,
                "clusters.html",
                shell(request, connection, run_row, rows=rows),
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
            return templates.TemplateResponse(
                request,
                "cluster.html",
                shell(request, connection, run_row, header=header, members=members, owners=owners),
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
                shell(request, connection, run_row, catalog=catalog, config_sha=settings.config_sha256),
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
                if "demonstration" in lowered:
                    status = 409
                elif "not in this run" in lowered or "no run" in lowered:
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
            return {"running": False, "message": "", "error": ""}
        try:
            data = json.loads(status_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {"running": False, "message": "", "error": ""}
        return {
            "running": bool(data.get("running")),
            "message": str(data.get("message") or ""),
            "error": str(data.get("error") or ""),
        }

    def write_status(**fields: object) -> dict:
        current = read_status()
        current.update(fields)
        status_path.parent.mkdir(parents=True, exist_ok=True)
        status_path.write_text(json.dumps(current), encoding="utf-8")
        return current

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
        drop_demo: str = Form(""),
        run_after: str = Form(""),
        focus: str = Form(""),
    ):
        from fraudledger.ingest.catalog import CUSTOM_KINDS, KNOWN_SOURCES, validate_http_url
        from fraudledger.ingest.pull import pull, pull_working
        from fraudledger.pipeline import execute_run

        if read_status().get("running"):
            return RedirectResponse("/sources", status_code=303)
        known = {item.key for item in KNOWN_SOURCES}
        kinds = {item[0] for item in CUSTOM_KINDS}
        remove_demo = drop_demo == "1"
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

        write_status(running=True, message=f"Starting {target_key}.", error="")

        def job() -> None:
            connection = None
            try:
                with _LOCK:
                    connection = db()
                    if target_key == "working":
                        text = pull_working(connection, settings)
                    else:
                        text = pull(
                            connection,
                            settings,
                            target_key,
                            url=target_url,
                            drop_demo=remove_demo,
                            focus=chosen_focus if target_key == "medicaid" else None,
                        )
                    if score:
                        facts = connection.execute("SELECT COUNT(*) FROM facts_medicaid").fetchone()[0]
                        if facts:
                            run_id = execute_run(connection, settings)
                            text += f" Run {run_id}."
                        else:
                            text += " No billing facts yet, so detection was not run."
                    write_status(running=False, message=text, error="")
            except SystemExit as exc:
                write_status(running=False, message="", error=str(exc))
            except Exception as exc:
                write_status(running=False, message="", error=str(exc))
            finally:
                if connection is not None:
                    connection.close()

        threading.Thread(target=job, name="fraudledger-pull", daemon=True).start()
        return RedirectResponse("/sources", status_code=303)

    @app.exception_handler(HTTPException)
    async def http_error(request: Request, exc: HTTPException):
        if request.url.path == "/health" or "application/json" in request.headers.get("accept", ""):
            return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)
        body = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><title>Fraud Ledger</title></head>
<body style="font-family: sans-serif; background:#f4f6f8; color:#142033; padding:40px;">
<p><a href="/">Fraud Ledger</a></p>
<h1>{exc.status_code}</h1>
<p>{exc.detail}</p>
</body></html>"""
        return HTMLResponse(body, status_code=exc.status_code)

    return app
