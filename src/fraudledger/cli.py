from __future__ import annotations

import argparse
import sys
import zipfile
from pathlib import Path

from fraudledger import __version__
from fraudledger.db import connect, refresh_facts_view
from fraudledger.ingest.demo import load_demo
from fraudledger.ingest.public import ingest_leie, ingest_medicaid, ingest_nppes, ingest_ownership
from fraudledger.pipeline import execute_run
from fraudledger.report.assemble import verify_run
from fraudledger.reviews import set_review
from fraudledger.settings import load_settings


def _open(args: argparse.Namespace):
    settings = load_settings(config=args.config, data_dir=args.data_dir)
    settings.ensure_dirs()
    con = connect(settings.db_path)
    refresh_facts_view(con, settings.data_dir / "parquet" / "facts")
    return settings, con


def _cmd_doctor(args: argparse.Namespace) -> int:
    import duckdb

    settings, con = _open(args)
    facts = con.execute("SELECT COUNT(*) FROM facts_medicaid").fetchone()[0]
    writable = os_access_write(settings.data_dir)
    print(f"fraudledger {__version__}")
    print(f"python {sys.version.split()[0]}")
    print(f"duckdb {duckdb.__version__}")
    print(f"data_dir {settings.data_dir}")
    print(f"writable {writable}")
    print(f"config {settings.config_path or 'package default'}")
    print(f"config_sha256 {settings.config_sha256}")
    print(f"database {settings.db_path}")
    print(f"facts {facts}")
    print(f"listen http://{settings.bind_host}:{settings.bind_port}/")
    con.close()
    return 0


def os_access_write(path: Path) -> bool:
    probe = path / ".write_probe"
    try:
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
        return True
    except OSError:
        return False


def _cmd_init(args: argparse.Namespace) -> int:
    settings, con = _open(args)
    print(f"Ready. Warehouse: {settings.db_path}")
    con.close()
    return 0


def _cmd_demo(args: argparse.Namespace) -> int:
    settings, con = _open(args)
    load_demo(con, settings)
    print("Loaded fictional demonstration facts (NPI range 199999xxxx).")
    if not args.no_run:
        run_id = execute_run(con, settings)
        print(f"Run {run_id}")
        print(f"Report {settings.data_dir / 'runs' / run_id / 'report.txt'}")
    con.close()
    return 0


def _cmd_ingest(args: argparse.Namespace) -> int:
    settings, con = _open(args)
    if args.source == "medicaid":
        count = ingest_medicaid(con, settings, path=args.path, url=args.url, drop_demo=args.drop_demo)
        print(f"Medicaid facts files in view: {count}")
    elif args.source == "leie":
        count = ingest_leie(con, settings, path=args.path, url=args.url)
        print(f"LEIE NPI rows: {count}")
    elif args.source == "nppes":
        count = ingest_nppes(con, settings, path=args.path, url=args.url)
        print(f"NPPES NPI rows staged: {count}")
    elif args.source == "ownership":
        count = ingest_ownership(con, settings, path=args.path, url=args.url, kind=args.kind)
        print(f"Ownership edges kept: {count}")
    else:
        con.close()
        raise SystemExit(f"Unknown ingest source: {args.source}")
    con.close()
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    settings, con = _open(args)
    run_id = execute_run(con, settings, run_id=args.run_id)
    print(run_id)
    con.close()
    return 0


def _cmd_refresh(args: argparse.Namespace) -> int:
    settings, con = _open(args)
    sources = settings.sources or {}
    did = False
    if sources.get("medicaid_url"):
        ingest_medicaid(con, settings, url=sources["medicaid_url"])
        did = True
    if sources.get("leie_url"):
        ingest_leie(con, settings, url=sources["leie_url"])
        did = True
    if sources.get("nppes_url"):
        ingest_nppes(con, settings, url=sources["nppes_url"])
        did = True
    if sources.get("hospice_owners_url"):
        ingest_ownership(con, settings, url=sources["hospice_owners_url"], kind="hospice_owners")
        did = True
    if sources.get("home_health_owners_url"):
        ingest_ownership(con, settings, url=sources["home_health_owners_url"], kind="home_health_owners")
        did = True
    if not did:
        print("No source URLs configured. Nothing downloaded.")
        con.close()
        return 0
    run_id = execute_run(con, settings)
    print(f"Run {run_id}")
    con.close()
    return 0


def _cmd_verify(args: argparse.Namespace) -> int:
    settings, con = _open(args)
    con.close()
    run_dir = settings.data_dir / "runs" / args.run_id
    verify_run(run_dir)
    print(f"Verified {run_dir}")
    return 0


def _cmd_serve(args: argparse.Namespace) -> int:
    import uvicorn

    from fraudledger.api.app import create_app

    settings = load_settings(config=args.config, data_dir=args.data_dir)
    settings.ensure_dirs()
    host = args.host or settings.bind_host
    port = args.port or settings.bind_port
    uvicorn.run(create_app(settings), host=host, port=port, workers=1, log_level="info")
    return 0


def _export_zip(settings, run_id: str) -> Path:
    if not run_id or "/" in run_id or "\\" in run_id or run_id in {".", ".."}:
        raise SystemExit("Refusing that run id.")
    run_dir = (settings.data_dir / "runs" / run_id).resolve()
    root = (settings.data_dir / "runs").resolve()
    if run_dir.parent != root or not run_dir.is_dir():
        raise SystemExit(f"No run directory: {run_id}")
    dest = settings.data_dir / "exports" / f"{run_id}.zip"
    dest.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(dest, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(run_dir.rglob("*")):
            if path.is_file():
                archive.write(path, path.relative_to(run_dir).as_posix())
    return dest


def _cmd_export(args: argparse.Namespace) -> int:
    settings, con = _open(args)
    con.close()
    dest = Path(args.out) if args.out else _export_zip(settings, args.run_id)
    if args.out:
        built = _export_zip(settings, args.run_id)
        Path(args.out).write_bytes(built.read_bytes())
        dest = Path(args.out)
    print(dest)
    print("This zip is an evidence package for a person to read. It does not submit a tip.")
    print("If a human decides a lead is worth sending, the public intake is https://america.gov/fraud")
    print("The Criminal Tip Rewards Pilot is discretionary. This tool does not compute a reward.")
    return 0


def _cmd_review(args: argparse.Namespace) -> int:
    settings, con = _open(args)
    set_review(con, args.provider_key, args.state, args.note or "")
    print(f"{args.provider_key} -> {args.state}")
    con.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="fraudledger",
        description="Local review workstation for public healthcare billing anomalies.",
    )
    parser.add_argument("--version", action="version", version=f"fraudledger {__version__}")
    parser.add_argument("--config", help="YAML config. Overrides FRAUDLEDGER_CONFIG.")
    parser.add_argument("--data-dir", help="Data directory. Overrides the config and FRAUDLEDGER_DATA_DIR.")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("doctor", help="Print versions, paths, and whether the data directory is writable.")
    sub.add_parser("init", help="Create the data directories and the warehouse.")

    demo = sub.add_parser("demo", help="Load fictional NPIs and, by default, run detection.")
    demo.add_argument("--no-run", action="store_true", help="Load the fixture without scoring it.")
    demo.set_defaults(func=_cmd_demo)

    ingest = sub.add_parser("ingest", help="Load a public file from disk or a URL.")
    ingest_sub = ingest.add_subparsers(dest="source", required=True)
    for name, help_text in (
        ("medicaid", "HHS Medicaid Provider Spending extract (CSV or Parquet)."),
        ("leie", "OIG LEIE CSV. Hard match is NPI only."),
        ("nppes", "NPPES monthly dissemination. Set the current dated zip URL yourself."),
        ("ownership", "PECOS hospice or home-health owner file."),
    ):
        command = ingest_sub.add_parser(name, help=help_text)
        command.add_argument("--path")
        command.add_argument("--url")
        command.set_defaults(func=_cmd_ingest)
        if name == "medicaid":
            command.add_argument(
                "--drop-demo",
                action="store_true",
                help="Remove fictional demonstration facts before publishing this extract into the view.",
            )
        if name == "ownership":
            command.add_argument("--kind", default="ownership", help="Label stored on the edge origin.")

    run = sub.add_parser("run", help="Score loaded facts and write an evidence bundle.")
    run.add_argument("--run-id")
    run.set_defaults(func=_cmd_run)

    refresh = sub.add_parser("refresh", help="Download configured source URLs, then score. No-ops if none are set.")
    refresh.set_defaults(func=_cmd_refresh)

    verify = sub.add_parser("verify-run", help="Recompute SHA-256 values in a run manifest.")
    verify.add_argument("run_id")
    verify.set_defaults(func=_cmd_verify)

    serve = sub.add_parser("serve", help="Start the localhost review UI.")
    serve.add_argument("--host")
    serve.add_argument("--port", type=int)
    serve.set_defaults(func=_cmd_serve)

    export = sub.add_parser("export-tip", help="Zip one run's evidence. Does not contact the government.")
    export.add_argument("run_id")
    export.add_argument("--out")
    export.set_defaults(func=_cmd_export)

    review = sub.add_parser("review", help="Set a review state. Dismissal does not delete flags.")
    review.add_argument("provider_key")
    review.add_argument("--state", required=True, choices=["unreviewed", "watch", "dismissed", "exported"])
    review.add_argument("--note", default="")
    review.set_defaults(func=_cmd_review)

    doctor = sub.choices["doctor"]
    doctor.set_defaults(func=_cmd_doctor)
    sub.choices["init"].set_defaults(func=_cmd_init)
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        code = args.func(args)
    except BrokenPipeError:
        code = 0
    raise SystemExit(code)


if __name__ == "__main__":
    main()
