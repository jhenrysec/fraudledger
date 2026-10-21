from __future__ import annotations

import csv
import re
from datetime import date, datetime
from pathlib import Path

import duckdb

from fraudledger.columns import normalize_header, resolve_columns
from fraudledger.db import refresh_facts_view
from fraudledger.graph.edges import qualifies, weight
from fraudledger.ingest.common import download, materialize, store_raw, utcnow
from fraudledger.settings import Settings


def _sql_path(path: Path) -> str:
    return "'" + path.as_posix().replace("'", "''") + "'"


def _record_snapshot(con: duckdb.DuckDBPyConnection, source_id: str, url: str, path: Path, digest: str, note: str) -> None:
    con.execute(
        """
        INSERT OR REPLACE INTO snapshots (snapshot_id, source_id, url, retrieved_at, byte_count, sha256, note)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [digest[:16], source_id, url, utcnow(), path.stat().st_size, digest, note],
    )


def _input_path(settings: Settings, path: str | None, url: str | None, label: str) -> tuple[Path, str]:
    if path:
        source = Path(path)
        if not source.is_file():
            raise SystemExit(f"{label} file not found: {source}")
        return source, path
    if url:
        dest = settings.data_dir / "tmp" / f"{label}.download"
        download(url, dest)
        return dest, url
    raise SystemExit(f"Pass --path or --url for {label}")


def ingest_medicaid(
    con: duckdb.DuckDBPyConnection,
    settings: Settings,
    path: str | None = None,
    url: str | None = None,
    drop_demo: bool = False,
) -> int:
    settings.ensure_dirs()
    source, locator = _input_path(settings, path, url, "medicaid")
    readable = materialize(source, settings.data_dir / "tmp" / "unpack")
    digest, note = store_raw(settings.data_dir, source, "medicaid", locator, readable.name)
    if readable.suffix.lower() == ".parquet":
        header_con = duckdb.connect()
        names = [row[0] for row in header_con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)",
            [str(readable)],
        ).fetchall()]
        header_con.close()
        reader = f"read_parquet('{readable.as_posix()}')"
    else:
        with readable.open(newline="", encoding="utf-8-sig") as handle:
            names = next(csv.reader(handle))
        reader = "read_csv(?, header=true, sample_size=-1, ignore_errors=false)"
    mapping = resolve_columns(names)
    servicing = mapping["servicing_npi"]
    beneficiaries = mapping["beneficiaries"]
    servicing_sql = f'"{servicing}"' if servicing else "NULL"
    beneficiaries_sql = f'"{beneficiaries}"' if beneficiaries else "NULL"
    out = settings.data_dir / "parquet" / "facts" / f"medicaid_{digest[:16]}.parquet"
    select = f"""
        SELECT
            lpad(CAST("{mapping['billing_npi']}" AS VARCHAR), 10, '0') AS billing_npi,
            lpad(CAST({servicing_sql} AS VARCHAR), 10, '0') AS servicing_npi,
            CAST("{mapping['hcpcs']}" AS VARCHAR) AS hcpcs,
            CAST("{mapping['month']}" AS DATE) AS month,
            CAST({beneficiaries_sql} AS DOUBLE) AS beneficiaries,
            CAST("{mapping['claim_lines']}" AS DOUBLE) AS claim_lines,
            CAST("{mapping['paid']}" AS DOUBLE) AS paid,
            'public' AS origin
        FROM {reader}
    """
    if readable.suffix.lower() == ".parquet":
        con.execute(f"COPY ({select}) TO {_sql_path(out)} (FORMAT PARQUET)")
    else:
        con.execute(f"COPY ({select}) TO {_sql_path(out)} (FORMAT PARQUET)", [str(readable)])
    if drop_demo:
        demo = settings.data_dir / "parquet" / "facts" / "demo.parquet"
        if demo.exists():
            demo.unlink()
        con.execute("DELETE FROM providers WHERE origin = 'demo'")
        con.execute("DELETE FROM leie WHERE origin = 'demo'")
        con.execute("DELETE FROM ownership_edges WHERE origin = 'demo'")
    count_files = refresh_facts_view(con, settings.data_dir / "parquet" / "facts")
    _record_snapshot(con, "medicaid", locator, source, digest, note)
    return count_files


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%m/%d/%Y", "%m%d%Y"):
        try:
            return datetime.strptime(text[:10] if fmt == "%Y-%m-%d" else text, fmt).date()
        except ValueError:
            continue
    return None


def ingest_leie(
    con: duckdb.DuckDBPyConnection,
    settings: Settings,
    path: str | None = None,
    url: str | None = None,
) -> int:
    settings.ensure_dirs()
    source, locator = _input_path(settings, path, url, "leie")
    readable = materialize(source, settings.data_dir / "tmp" / "unpack")
    digest, note = store_raw(settings.data_dir, source, "leie", locator, readable.name)
    inserted = 0
    with readable.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise SystemExit("LEIE file has no header")
        lookup = {normalize_header(name): name for name in reader.fieldnames}
        npi_col = lookup.get("npi")
        if not npi_col:
            raise SystemExit(f"LEIE file has no NPI column. Headers: {reader.fieldnames[:20]}")
        last = lookup.get("lastname")
        first = lookup.get("firstname")
        bus = lookup.get("busname")
        excl_type = lookup.get("excltype")
        excl_date = lookup.get("excldate")
        con.execute("DELETE FROM leie WHERE origin = 'public'")
        batch = []
        for row in reader:
            raw_npi = re.sub(r"\D", "", row.get(npi_col) or "")
            if len(raw_npi) != 10:
                continue
            name = " ".join(
                part.strip()
                for part in (
                    row.get(first) if first else "",
                    row.get(last) if last else "",
                    row.get(bus) if bus else "",
                )
                if part and part.strip()
            )
            batch.append(
                (
                    raw_npi,
                    name or None,
                    (row.get(excl_type) or "").strip() if excl_type else None,
                    _parse_date(row.get(excl_date) if excl_date else None),
                    "public",
                )
            )
            if len(batch) >= 5000:
                con.executemany(
                    "INSERT INTO leie (npi, name, excl_type, excl_date, origin) VALUES (?, ?, ?, ?, ?)",
                    batch,
                )
                inserted += len(batch)
                batch = []
        if batch:
            con.executemany(
                "INSERT INTO leie (npi, name, excl_type, excl_date, origin) VALUES (?, ?, ?, ?, ?)",
                batch,
            )
            inserted += len(batch)
    _record_snapshot(con, "leie", locator, source, digest, note)
    return inserted


def ingest_ownership(
    con: duckdb.DuckDBPyConnection,
    settings: Settings,
    path: str | None = None,
    url: str | None = None,
    kind: str = "ownership",
) -> int:
    settings.ensure_dirs()
    source, locator = _input_path(settings, path, url, kind)
    readable = materialize(source, settings.data_dir / "tmp" / "unpack")
    digest, note = store_raw(settings.data_dir, source, kind, locator, readable.name)
    inserted = 0
    with readable.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise SystemExit("Ownership file has no header")
        lookup = {normalize_header(name): name for name in reader.fieldnames}

        def pick(*candidates: str) -> str | None:
            for candidate in candidates:
                if candidate in lookup:
                    return lookup[candidate]
            return None

        associate = pick("associate_id_owner", "associate_id", "owner_associate_id")
        provider_npi = pick("npi", "billing_npi", "organization_npi")
        enrollment = pick("enrollment_id", "enrollment")
        percent = pick("percentage_ownership", "percentage", "percent_ownership")
        role = pick("role_code_owner", "owner_role_code", "role_code", "role")
        owner_name = pick("organization_name_owner", "owner_name", "associate_name")
        org_name = pick("organization_name", "org_name", "provider_name")
        if not associate or not (provider_npi or enrollment):
            raise SystemExit(
                "Ownership file needs an associate id and an NPI or enrollment id. "
                f"Headers: {reader.fieldnames[:24]}"
            )
        con.execute("DELETE FROM ownership_edges WHERE origin = ?", [f"public:{kind}"])
        batch = []
        for row in reader:
            owner_key = (row.get(associate) or "").strip()
            if not owner_key:
                continue
            npi = re.sub(r"\D", "", row.get(provider_npi) or "") if provider_npi else ""
            enrollment_id = (row.get(enrollment) or "").strip() if enrollment else ""
            provider_key = npi.zfill(10) if len(npi) == 10 else enrollment_id
            if not provider_key:
                continue
            raw_percent = (row.get(percent) or "").strip() if percent else ""
            try:
                percentage = float(raw_percent) if raw_percent else None
            except ValueError:
                percentage = None
            role_code = (row.get(role) or "").strip() if role else ""
            if not qualifies(percentage, role_code, settings):
                continue
            edge_weight = weight(percentage, role_code, settings)
            strength = settings.role_strength.get(role_code, settings.default_role_strength)
            name = ""
            if owner_name:
                name = (row.get(owner_name) or "").strip()
            batch.append(
                (
                    owner_key,
                    name or None,
                    provider_key,
                    percentage,
                    role_code or None,
                    strength,
                    edge_weight,
                    None,
                    f"{kind}:{enrollment_id}:{owner_key}:{role_code}",
                    f"public:{kind}",
                )
            )
            if org_name and npi:
                con.execute(
                    """
                    INSERT INTO providers (
                        provider_key, npi, enrollment_id, ccn, name, entity_type, taxonomy,
                        address, city, state, zip, enumeration_date, origin
                    ) VALUES (?, ?, ?, NULL, ?, 'organization', NULL, NULL, NULL, NULL, NULL, NULL, 'public')
                    ON CONFLICT (provider_key) DO NOTHING
                    """,
                    [provider_key, npi.zfill(10) if len(npi) == 10 else None, enrollment_id or None, (row.get(org_name) or "").strip() or None],
                )
        if batch:
            con.executemany(
                """
                INSERT INTO ownership_edges (
                    owner_key, owner_name, provider_key, percentage, role_code,
                    role_strength, edge_weight, effective_date, edge_source_id, origin
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                batch,
            )
            inserted = len(batch)
    _record_snapshot(con, kind, locator, source, digest, note)
    return inserted


def ingest_nppes(
    con: duckdb.DuckDBPyConnection,
    settings: Settings,
    path: str | None = None,
    url: str | None = None,
) -> int:
    """Load NPPES enumeration dates and practice locations.

    The monthly CMS file name includes a date, so the operator sets the
    current zip URL. Only 10-digit NPIs are kept. Demonstration rows keep
    their origin so a later public file cannot silently rebrand them.
    """
    settings.ensure_dirs()
    source, locator = _input_path(settings, path, url, "nppes")
    readable = materialize(source, settings.data_dir / "tmp" / "unpack")
    digest, note = store_raw(settings.data_dir, source, "nppes", locator, readable.name)
    if readable.suffix.lower() == ".parquet":
        header_con = duckdb.connect()
        names = [
            row[0]
            for row in header_con.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)",
                [str(readable)],
            ).fetchall()
        ]
        header_con.close()
        reader = f"read_parquet({_sql_path(readable)})"
        params: list[str] = []
    else:
        with readable.open(newline="", encoding="utf-8-sig") as handle:
            names = next(csv.reader(handle))
        reader = "read_csv(?, header=true, sample_size=20000, all_varchar=true, ignore_errors=true)"
        params = [str(readable)]
    lookup = {normalize_header(name): name for name in names}

    def pick(*candidates: str) -> str | None:
        for candidate in candidates:
            if candidate in lookup:
                return lookup[candidate]
        return None

    npi = pick("npi")
    if not npi:
        raise SystemExit(f"NPPES file has no NPI column. Headers: {names[:20]}")
    enumerated = pick("provider_enumeration_date", "enumeration_date")
    org = pick(
        "provider_organization_name_legal_business_name",
        "provider_organization_name",
        "organization_name",
    )
    last = pick("provider_last_name_legal_name", "provider_last_name", "last_name")
    first = pick("provider_first_name", "first_name")
    address = pick("provider_first_line_business_practice_location_address", "address")
    city = pick("provider_business_practice_location_address_city_name", "city")
    state = pick("provider_business_practice_location_address_state_name", "state")
    postal = pick("provider_business_practice_location_address_postal_code", "zip", "postal_code")
    taxonomy = pick("healthcare_provider_taxonomy_code_1", "taxonomy")
    entity = pick("entity_type_code", "entity_type")

    def text_col(column: str | None) -> str:
        if not column:
            return "CAST(NULL AS VARCHAR)"
        return f'NULLIF(TRIM(CAST("{column}" AS VARCHAR)), \'\')'

    if enumerated:
        enum_sql = f"""COALESCE(
            try_strptime("{enumerated}", '%m/%d/%Y'),
            try_strptime("{enumerated}", '%Y-%m-%d'),
            try_strptime("{enumerated}", '%Y%m%d')
        )::DATE"""
    else:
        enum_sql = "CAST(NULL AS DATE)"
    if org or last or first:
        name_sql = f"NULLIF(TRIM(CONCAT_WS(' ', {text_col(first)}, {text_col(last)}, {text_col(org)})), '')"
    else:
        name_sql = "CAST(NULL AS VARCHAR)"
    if entity:
        entity_sql = f"""CASE {text_col(entity)}
            WHEN '1' THEN 'individual'
            WHEN '2' THEN 'organization'
            ELSE {text_col(entity)}
        END"""
    else:
        entity_sql = "CAST(NULL AS VARCHAR)"
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE nppes_stage AS
        SELECT
            lpad(regexp_replace(CAST("{npi}" AS VARCHAR), '[^0-9]', '', 'g'), 10, '0') AS provider_key,
            {name_sql} AS name,
            {entity_sql} AS entity_type,
            {text_col(taxonomy)} AS taxonomy,
            {text_col(address)} AS address,
            {text_col(city)} AS city,
            {text_col(state)} AS state,
            {text_col(postal)} AS zip,
            {enum_sql} AS enumeration_date
        FROM {reader}
        WHERE length(regexp_replace(CAST("{npi}" AS VARCHAR), '[^0-9]', '', 'g')) = 10
        """,
        params,
    )
    con.execute(
        """
        INSERT INTO providers (
            provider_key, npi, enrollment_id, ccn, name, entity_type, taxonomy,
            address, city, state, zip, enumeration_date, origin
        )
        SELECT
            provider_key, provider_key, NULL, NULL, name, entity_type, taxonomy,
            address, city, state, zip, enumeration_date, 'public'
        FROM nppes_stage
        ON CONFLICT (provider_key) DO UPDATE SET
            name = COALESCE(excluded.name, providers.name),
            entity_type = COALESCE(excluded.entity_type, providers.entity_type),
            taxonomy = COALESCE(excluded.taxonomy, providers.taxonomy),
            address = COALESCE(excluded.address, providers.address),
            city = COALESCE(excluded.city, providers.city),
            state = COALESCE(excluded.state, providers.state),
            zip = COALESCE(excluded.zip, providers.zip),
            enumeration_date = COALESCE(excluded.enumeration_date, providers.enumeration_date)
        """
    )
    inserted = con.execute("SELECT COUNT(*) FROM nppes_stage").fetchone()[0]
    _record_snapshot(con, "nppes", locator, source, digest, note)
    return int(inserted)
