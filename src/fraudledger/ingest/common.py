from __future__ import annotations

import shutil
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from fraudledger.hashing import sha256_file


def download(url: str, dest: Path) -> str:
    dest.parent.mkdir(parents=True, exist_ok=True)
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "fraudledger/1.0 (public-data research)"},
    )
    with urllib.request.urlopen(request, timeout=120) as response, dest.open("wb") as handle:
        shutil.copyfileobj(response, handle)
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
