"""Known public files, plus resolution of the current download URL."""

from __future__ import annotations

import json
import re
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass

from fraudledger.ingest.common import download
from fraudledger.settings import Settings

CMS_CATALOG = "https://data.cms.gov/data.json"
PROVIDER_CATALOG = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
NPPES_INDEX = "https://download.cms.gov/nppes/NPI_Files.html"
MEDICAID_PAGE = "https://opendata.hhs.gov/datasets/medicaid-provider-spending/"
LEIE_CSV = "https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv"

CATALOG_MAX_AGE = 24 * 60 * 60


@dataclass(frozen=True)
class KnownSource:
    key: str
    title: str
    summary: str
    page: str
    size_note: str


KNOWN_SOURCES: tuple[KnownSource, ...] = (
    KnownSource(
        "medicaid",
        "Medicaid Provider Spending",
        "HHS T-MSIS extract: billing NPI, HCPCS, and month. This is the file the rules score.",
        MEDICAID_PAGE,
        "Multi-gigabyte download. The working set keeps hospice, home health, and personal-care procedure codes and drops the rest.",
    ),
    KnownSource(
        "leie",
        "OIG LEIE",
        "Excluded individuals and entities. A hard match uses the NPI column only.",
        "https://oig.hhs.gov/exclusions/exclusions_list.asp",
        "About 10 MB.",
    ),
    KnownSource(
        "nppes",
        "NPPES monthly file",
        "Enumeration dates and practice locations. The CMS index names the current V.2 zip.",
        NPPES_INDEX,
        "About 1 GB compressed. The index is sometimes offline for maintenance.",
    ),
    KnownSource(
        "hospice",
        "Hospice owners and enrollments",
        "Latest PECOS owner file plus the enrollment file that carries the NPI.",
        "https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/hospice-all-owners",
        "Two CSVs from the CMS catalog. Much smaller than the Medicaid extract.",
    ),
    KnownSource(
        "home-health",
        "Home health owners and enrollments",
        "Latest PECOS owner file plus the enrollment file that carries the NPI.",
        "https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/home-health-agency-all-owners",
        "Two CSVs from the CMS catalog. Much smaller than the Medicaid extract.",
    ),
    KnownSource(
        "medicare-hospice",
        "Medicare hospice use",
        "CMS post-acute hospice file: stays, service days, and routine-home-care share, joined by certification number.",
        "https://data.cms.gov/provider-summary-by-type-of-service/medicare-post-acute-care-hospice/medicare-post-acute-care-utilization-hospice-by-geography-and-provider",
        "The latest provider year. Much smaller than the Medicaid extract.",
    ),
    KnownSource(
        "medicare-home-health",
        "Medicare home health use",
        "CMS post-acute home health file: episodes and visits, joined by certification number.",
        "https://data.cms.gov/provider-summary-by-type-of-service/medicare-post-acute-care-hospice",
        "The latest provider year. Much smaller than the Medicaid extract.",
    ),
    KnownSource(
        "hospice-care",
        "Hospice Care Compare",
        "Live-discharge percentages from the CMS hospice provider file. Used only as a corroborating indicator.",
        "https://data.cms.gov/provider-data/topics/hospice-care",
        "One CSV. Much smaller than the Medicaid extract.",
    ),
    KnownSource(
        "home-health-care",
        "Home health Care Compare",
        "Potentially preventable readmissions from the CMS home health agency file.",
        "https://data.cms.gov/provider-data/topics/home-health-services",
        "One CSV. Much smaller than the Medicaid extract.",
    ),
)

CUSTOM_KINDS: tuple[tuple[str, str], ...] = (
    ("medicaid", "Medicaid spending (CSV, ZIP, or Parquet)"),
    ("leie", "OIG LEIE CSV"),
    ("nppes", "NPPES dissemination ZIP or CSV"),
    ("hospice-owners", "Hospice owner CSV"),
    ("hospice-enrollments", "Hospice enrollment CSV"),
    ("home-health-owners", "Home health owner CSV"),
    ("home-health-enrollments", "Home health enrollment CSV"),
    ("medicare-hospice", "Medicare hospice provider CSV"),
    ("medicare-home-health", "Medicare home health provider CSV"),
    ("hospice-care", "Hospice Care Compare CSV"),
    ("home-health-care", "Home health Care Compare CSV"),
)

CONFIG_KEYS = {
    "medicaid": "medicaid_url",
    "leie": "leie_url",
    "nppes": "nppes_url",
    "hospice-owners": "hospice_owners_url",
    "hospice-enrollments": "hospice_enrollments_url",
    "home-health-owners": "home_health_owners_url",
    "home-health-enrollments": "home_health_enrollments_url",
    "medicare-hospice": "medicare_hospice_url",
    "medicare-home-health": "medicare_home_health_url",
    "hospice-care": "hospice_care_url",
    "home-health-care": "home_health_care_url",
}


def validate_http_url(url: str) -> str:
    text = (url or "").strip()
    if len(text) > 2000:
        raise SystemExit("URL is too long.")
    parsed = urllib.parse.urlparse(text)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise SystemExit("URL must start with http:// or https://")
    return text


def configured_url(settings: Settings, key: str) -> str:
    name = CONFIG_KEYS[key]
    value = str((settings.sources or {}).get(name) or "").strip()
    return validate_http_url(value) if value else ""


def medicaid_url_from_html(html: str) -> str:
    text = html.replace("\\/", "/")
    rank = {"parquet": 0, "csv.zip": 1, "csv": 2}
    found: list[tuple[str, int, str]] = []
    pattern = re.compile(
        r"https://[^\"'\s<>]+/medicaid-provider-spending/"
        r"(\d{4}-\d{2}-\d{2})/dataset/medicaid-provider-spending\.(parquet|csv\.zip|csv)\b"
    )
    for match in pattern.finditer(text):
        found.append((match.group(1), rank.get(match.group(2), 9), match.group(0)))
    if not found:
        raise SystemExit(
            "The HHS dataset page did not list a Parquet or CSV download. "
            f"Open {MEDICAID_PAGE} and paste the file URL."
        )
    found.sort(key=lambda item: (item[0], -item[1]))
    return found[-1][2]


def nppes_monthly_url_from_html(html: str) -> str:
    text = html.replace("\\/", "/")
    candidates: list[str] = []
    for match in re.finditer(
        r"<a\s+[^>]*href=[\"']([^\"']+\.zip)[\"'][^>]*>(.*?)</a>",
        text,
        flags=re.I | re.S,
    ):
        href = match.group(1).strip()
        label = re.sub(r"<[^>]+>", " ", match.group(2))
        blob = f"{href} {label}".lower()
        if "deactiv" in blob or "weekly" in blob:
            continue
        candidates.append(urllib.parse.urljoin(NPPES_INDEX, href))
    if candidates:
        return candidates[0]
    if re.search(r"down for maintenance", text, re.I):
        raise SystemExit(
            "The CMS NPI file index is down for maintenance and did not list a monthly zip. "
            f"Paste the current URL from {NPPES_INDEX}"
        )
    raise SystemExit(
        "The CMS NPI file index did not list a monthly zip. "
        f"Paste the current URL from {NPPES_INDEX}"
    )


def latest_csv_url(catalog: dict | list, title_prefix: str) -> str:
    datasets = catalog.get("dataset") if isinstance(catalog, dict) else catalog
    best: tuple[str, str] | None = None
    for item in datasets or []:
        title = str(item.get("title") or "")
        if not title.lower().startswith(title_prefix.lower()):
            continue
        modified = str(item.get("modified") or "")
        for dist in item.get("distribution") or []:
            url = str(dist.get("downloadURL") or "")
            if not url.lower().split("?", 1)[0].endswith(".csv"):
                continue
            candidate = (modified, url)
            if best is None or candidate > best:
                best = candidate
    if best is None:
        raise SystemExit(f"No CSV in the CMS catalog for {title_prefix}. Paste the file URL.")
    return best[1]


def latest_pac_url(catalog: dict | list, setting: str) -> str:
    """Latest provider-level post-acute CSV. Service-detail files are skipped."""
    datasets = catalog.get("dataset") if isinstance(catalog, dict) else catalog
    needle = f"{setting} by Geography and Provider".lower()
    best: tuple[int, str, str] | None = None
    for item in datasets or []:
        title = str(item.get("title") or "")
        lowered = title.lower()
        if needle not in lowered or "service" in lowered:
            continue
        found = re.findall(r"20\d\d", title)
        year = int(found[-1]) if found else 0
        modified = str(item.get("modified") or "")
        for dist in item.get("distribution") or []:
            url = str(dist.get("downloadURL") or "")
            if not url.lower().split("?", 1)[0].endswith(".csv"):
                continue
            candidate = (year, modified, url)
            if best is None or candidate > best:
                best = candidate
    if best is None:
        raise SystemExit(f"No Medicare {setting} provider CSV in the CMS catalog.")
    return best[2]


def provider_csv_url(catalog: dict | list, title: str) -> str:
    """CSV download for one exact title in the CMS provider-data catalog."""
    items = catalog if isinstance(catalog, list) else []
    for item in items:
        if str(item.get("title") or "") != title:
            continue
        for dist in item.get("distribution") or []:
            node = dist.get("data") if isinstance(dist, dict) and isinstance(dist.get("data"), dict) else dist
            if not isinstance(node, dict):
                continue
            url = str(node.get("downloadURL") or "")
            if url.lower().split("?", 1)[0].endswith(".csv"):
                return url
    raise SystemExit(f"No CSV in the CMS provider catalog for {title}. Paste the file URL.")


def _fetch_text(url: str) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": "fraudledger/1.0 (public-data research)"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return response.read().decode("utf-8", "replace")


def load_cms_catalog(settings: Settings) -> dict:
    settings.ensure_dirs()
    cache = settings.data_dir / "tmp" / "cms-data.json"
    if cache.is_file() and time.time() - cache.stat().st_mtime < CATALOG_MAX_AGE:
        return json.loads(cache.read_text(encoding="utf-8"))
    dest = settings.data_dir / "tmp" / "cms-data.json.download"
    download(CMS_CATALOG, dest)
    raw = dest.read_bytes()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw.decode("cp1252")
    cache.write_text(text, encoding="utf-8")
    return json.loads(text)


def load_provider_catalog(settings: Settings) -> list:
    settings.ensure_dirs()
    cache = settings.data_dir / "tmp" / "provider-data.json"
    if cache.is_file() and time.time() - cache.stat().st_mtime < CATALOG_MAX_AGE:
        return json.loads(cache.read_text(encoding="utf-8"))
    dest = settings.data_dir / "tmp" / "provider-data.json.download"
    download(PROVIDER_CATALOG, dest)
    text = dest.read_text(encoding="utf-8-sig")
    cache.write_text(text, encoding="utf-8")
    parsed = json.loads(text)
    if not isinstance(parsed, list):
        raise SystemExit("The CMS provider catalog did not return a dataset list.")
    return parsed


def resolve_file_url(settings: Settings, key: str, override: str | None = None) -> str:
    if override:
        return validate_http_url(override)
    configured = configured_url(settings, key)
    if configured:
        return configured
    if key == "leie":
        return LEIE_CSV
    if key == "medicaid":
        return medicaid_url_from_html(_fetch_text(MEDICAID_PAGE))
    if key == "nppes":
        return nppes_monthly_url_from_html(_fetch_text(NPPES_INDEX))
    if key == "hospice-care":
        return provider_csv_url(load_provider_catalog(settings), "Hospice - Provider Data")
    if key == "home-health-care":
        return provider_csv_url(load_provider_catalog(settings), "Home Health Care Agencies")
    catalog = load_cms_catalog(settings)
    prefixes = {
        "hospice-owners": "Hospice All Owners",
        "hospice-enrollments": "Hospice Enrollments",
        "home-health-owners": "Home Health Agency All Owners",
        "home-health-enrollments": "Home Health Agency Enrollments",
    }
    if key == "medicare-hospice":
        return latest_pac_url(catalog, "Hospice")
    if key == "medicare-home-health":
        return latest_pac_url(catalog, "Home Health Agency")
    if key not in prefixes:
        raise SystemExit(f"Unknown source: {key}")
    return latest_csv_url(catalog, prefixes[key])
