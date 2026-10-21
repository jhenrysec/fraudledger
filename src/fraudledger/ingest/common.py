"""Shared download and text helpers."""

from __future__ import annotations

import shutil
import time
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from fraudledger.hashing import sha256_file

_reporter = None
_label = "Download"


def set_progress_reporter(fn) -> None:
    """fn(message, received, total). total is None when the server omits a length."""
    global _reporter
    _reporter = fn


def set_progress_label(label: str) -> None:
    global _label
    _label = label or "Download"


def report_progress(message: str, received: int | None = None, total: int | None = None) -> None:
    if _reporter is not None:
        _reporter(message, received, total)


def percent_of(received: int | None, total: int | None) -> int | None:
    if not received or not total or total <= 0:
        return 0 if total else None
    return max(0, min(100, int(received * 100 / total)))


def text_encoding(path: Path) -> str:
    """UTF-8 when the file is UTF-8. CMS enrollment files are Windows-1252.

    Scanning is capped so a multi-gigabyte extract is not read twice in full.
    The hospice and home-health files that fail UTF-8 are well under this cap.
    """
    cap = 40 * 1024 * 1024
    with path.open("rb") as handle:
        raw = handle.read(cap)
    if raw.startswith((b"\xff\xfe", b"\xfe\xff")):
        return "utf-16"
    try:
        raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        return "cp1252"
    return "utf-8-sig"


def open_text(path: Path):
    return path.open(newline="", encoding=text_encoding(path))


def download(url: str, dest: Path, label: str | None = None) -> str:
    dest.parent.mkdir(parents=True, exist_ok=True)
    name = label or _label or urllib.parse.urlparse(url).path.rsplit("/", 1)[-1] or "Download"
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "fraudledger/1.0 (public-data research)"},
    )
    with urllib.request.urlopen(request, timeout=120) as response, dest.open("wb") as handle:
        total_header = response.headers.get("Content-Length")
        total = int(total_header) if total_header and total_header.isdigit() else None
        received = 0
        last = 0.0
        report_progress(name, 0, total)
        while True:
            chunk = response.read(256 * 1024)
            if not chunk:
                break
            handle.write(chunk)
            received += len(chunk)
            now = time.monotonic()
            if now - last >= 0.4:
                report_progress(name, received, total)
                last = now
        report_progress(f"Reading {name}", received, total or received)
    return sha256_file(dest)


def store_raw(settings_data: Path, source_path: Path, source_id: str, url: str, note: str) -> tuple[str, str]:
    digest = sha256_file(source_path)
    bucket = settings_data / "raw" / digest[:2]
    bucket.mkdir(parents=True, exist_ok=True)
    stored = bucket / digest
    if not stored.exists():
        shutil.copyfile(source_path, stored)
    return digest, note or source_id


def materialize(path: Path, tmp_dir: Path) -> Path:
    """Return a CSV or Parquet path, extracting a zip when needed."""
    if path.suffix.lower() != ".zip":
        return path
    tmp_dir.mkdir(parents=True, exist_ok=True)
    target = tmp_dir / path.stem
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    with zipfile.ZipFile(path) as archive:
        archive.extractall(target)
    candidates = [
        item
        for item in target.rglob("*")
        if item.suffix.lower() in {".csv", ".parquet", ".txt"} and item.is_file()
    ]
    if not candidates:
        raise SystemExit(f"No CSV or Parquet inside {path}")
    candidates.sort(key=lambda item: item.stat().st_size, reverse=True)
    return candidates[0]


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)