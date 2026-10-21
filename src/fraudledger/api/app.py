from __future__ import annotations

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
