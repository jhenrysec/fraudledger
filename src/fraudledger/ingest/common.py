"""Shared download and text helpers."""

from __future__ import annotations

import shutil
import time
import urllib.error
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


_HEADER_PROFILES = (
    {
        "User-Agent": "fraudledger/1.0 (public-data research)",
        "Accept": "*/*",
    },
    {
        "User-Agent": "curl/8.5.0",
        "Accept": "*/*",
    },
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    },
)


def open_remote(url: str, extra: dict | None = None, timeout: int = 120, method: str | None = None):
    """Open a public file. CMS sometimes answers 403 to one client type and 200 to another."""
    last: urllib.error.HTTPError | None = None
    for index, profile in enumerate(_HEADER_PROFILES):
        headers = dict(profile)
        if extra:
            headers.update(extra)
        if index == len(_HEADER_PROFILES) - 1:
            origin = urllib.parse.urlparse(url)
            if origin.scheme and origin.netloc:
                headers.setdefault("Referer", f"{origin.scheme}://{origin.netloc}/")
        request = urllib.request.Request(url, headers=headers, method=method)
        try:
            return urllib.request.urlopen(request, timeout=timeout)
        except urllib.error.HTTPError as exc:
            last = exc
            try:
                exc.close()
            except OSError:
                pass
            if exc.code not in {403, 429, 503} or index + 1 == len(_HEADER_PROFILES):
                break
            time.sleep(0.8)
    code = last.code if last is not None else "error"
    raise SystemExit(f"The download failed (HTTP {code}): {url}") from last


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
    encoding = text_encoding(path)
    # Five Windows-1252 bytes are undefined. Python reports those as 'charmap'.
    if encoding == "cp1252":
        return path.open(newline="", encoding="cp1252", errors="replace")
    return path.open(newline="", encoding=encoding)


def file_kind(path: Path) -> str:
    """parquet, zip, or text. The saved name is often *.download, so the suffix is not enough."""
    with path.open("rb") as handle:
        magic = handle.read(8)
    if magic.startswith(b"PAR1"):
        return "parquet"
    if magic.startswith((b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")):
        return "zip"
    return "text"


def remote_size(url: str) -> int | None:
    try:
        with open_remote(url, timeout=60, method="HEAD") as response:
            value = response.headers.get("Content-Length")
            if value and value.isdigit() and int(value) > 0:
                return int(value)
    except (OSError, SystemExit, ValueError):
        pass
    try:
        with open_remote(url, extra={"Range": "bytes=0-0"}, timeout=60) as response:
            header = response.headers.get("Content-Range") or ""
            total = header.rsplit("/", 1)[-1]
            if total.isdigit() and int(total) > 0:
                return int(total)
    except (OSError, SystemExit, ValueError):
        return None
    return None


def download(url: str, dest: Path, label: str | None = None) -> str:
    dest.parent.mkdir(parents=True, exist_ok=True)
    name = label or _label or urllib.parse.urlparse(url).path.rsplit("/", 1)[-1] or "Download"
    if dest.is_file():
        have = dest.stat().st_size
        expected = remote_size(url)
        if expected and have == expected:
            report_progress(f"Using the completed download of {name}", have, expected)
            return sha256_file(dest)
    with open_remote(url, timeout=120) as response, dest.open("wb") as handle:
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
    if file_kind(path) != "zip":
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