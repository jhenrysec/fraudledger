from __future__ import annotations

import csv
import re
from datetime import date, datetime
from pathlib import Path

import duckdb

from fraudledger.columns import normalize_header, resolve_columns
from fraudledger.db import refresh_facts_view
from fraudledger.graph.edges import qualifies, weight
from fraudledger.ingest.common import (
    download,
    file_kind,
    materialize,
    open_text,
    store_raw,
    text_encoding,
    utcnow,
)
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
        return source, url or str(source)
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
    focus: str | None = None,
) -> int:
    settings.ensure_dirs()
    source, locator = _input_path(settings, path, url, "medicaid")
    readable = materialize(source, settings.data_dir / "tmp" / "unpack")
    digest, note = store_raw(settings.data_dir, source, "medicaid", locator, readable.name)
    if file_kind(readable) == "parquet":
        header_con = duckdb.connect()
        names = [row[0] for row in header_con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)",
            [str(readable)],
        ).fetchall()]
        header_con.close()
        reader = f"read_parquet('{readable.as_posix()}')"
    else:
        enc = text_encoding(readable)
        with readable.open(newline="", encoding=enc) as handle:
            names = next(csv.reader(handle))
        duck = "cp1252" if enc == "cp1252" else "utf-8"
        reader = f"read_csv(?, header=true, sample_size=-1, ignore_errors=false, encoding='{duck}')"
    mapping = resolve_columns(names)
    servicing = mapping["servicing_npi"]
    beneficiaries = mapping["beneficiaries"]
    servicing_sql = f'"{servicing}"' if servicing else "NULL"
    beneficiaries_sql = f'"{beneficiaries}"' if beneficiaries else "NULL"
    month_expr = _as_month(mapping["month"])
    billing_text = f"trim(CAST(\"{mapping['billing_npi']}\" AS VARCHAR))"
    where = f"\nWHERE {month_expr} IS NOT NULL\n  AND {billing_text} <> ''"
    if focus == "priority":
        from fraudledger.ingest.focus import priority_in_list

        where += (
            f'\n  AND upper(trim(CAST("{mapping["hcpcs"]}" AS VARCHAR))) '
            f"IN ({priority_in_list()})"
        )
    elif focus:
        raise SystemExit("Unknown focus. Use priority, or omit it to keep every procedure code.")
    out = settings.data_dir / "parquet" / "facts" / f"medicaid_{digest[:16]}.parquet"
    partial = out.with_name(out.name + ".partial")
    select = f"""
        SELECT
            lpad(CAST("{mapping['billing_npi']}" AS VARCHAR), 10, '0') AS billing_npi,
            lpad(CAST({servicing_sql} AS VARCHAR), 10, '0') AS servicing_npi,
            CAST("{mapping['hcpcs']}" AS VARCHAR) AS hcpcs,
            {month_expr} AS month,
            CAST({beneficiaries_sql} AS DOUBLE) AS beneficiaries,
            CAST("{mapping['claim_lines']}" AS DOUBLE) AS claim_lines,
            CAST("{mapping['paid']}" AS DOUBLE) AS paid,
            'public' AS origin
        FROM {reader}{where}
    """
    try:
        if file_kind(readable) == "parquet":
            con.execute(f"COPY ({select}) TO {_sql_path(partial)} (FORMAT PARQUET)")
        else:
            con.execute(f"COPY ({select}) TO {_sql_path(partial)} (FORMAT PARQUET)", [str(readable)])
        partial.replace(out)
    except Exception:
        if partial.exists():
            partial.unlink()
        raise
    from fraudledger.ingest.cleanup import purge_reserved

    purge_reserved(con, settings)
    count_files = refresh_facts_view(con, settings.data_dir / "parquet" / "facts")
    _record_snapshot(con, "medicaid", locator, source, digest, note)
    return count_files


def _as_month(column: str) -> str:
    """Medicaid publishes CLAIM_FROM_MONTH as YYYY-MM. A full date is also accepted."""
    text = f"trim(CAST(\"{column}\" AS VARCHAR))"
    return (
        "CAST(COALESCE("
        f"try_strptime({text}, '%Y-%m-%d'), "
        f"try_strptime({text}, '%Y-%m'), "
        f"try_strptime({text}, '%Y%m%d'), "
        f"try_strptime({text}, '%Y%m'), "
        f"try_strptime({text}, '%m/%d/%Y')"
        ") AS DATE)"
    )


def _name_key(value: str | None) -> str | None:
    text = re.sub(r"[^A-Za-z]+", " ", value or "").strip().upper()
    return text or None


def _state_key(value: str | None) -> str | None:
    text = re.sub(r"[^A-Za-z]", "", value or "").upper()
    if len(text) != 2:
        return None
    return text


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y%m%d", "%Y%m", "%m/%d/%Y", "%m%d%Y"):
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
    with open_text(readable) as handle:
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
        state_col = lookup.get("state")
        middle_col = lookup.get("midname")
        reinstate_col = lookup.get("reindate")
        con.execute("DELETE FROM leie WHERE origin = 'public'")
        batch = []

        def add(raw_npi: str | None, row: dict) -> None:
            first_name = _name_key(row.get(first) if first else None)
            last_name = _name_key(row.get(last) if last else None)
            state_name = _state_key(row.get(state_col) if state_col else None)
            name = " ".join(
                part
                for part in (
                    first_name,
                    last_name,
                    (row.get(bus) or "").strip() if bus else "",
                )
                if part
            )
            batch.append(
                (
                    raw_npi,
                    name or None,
                    (row.get(excl_type) or "").strip() if excl_type else None,
                    _parse_date(row.get(excl_date) if excl_date else None),
                    first_name,
                    last_name,
                    state_name,
                    _name_key(row.get(middle_col) if middle_col else None),
                    _parse_date(row.get(reinstate_col) if reinstate_col else None),
                    "public",
                )
            )

        for row in reader:
            raw_npi = re.sub(r"\D", "", row.get(npi_col) or "")
            named = _name_key(row.get(first) if first else None) and _name_key(row.get(last) if last else None)
            if len(raw_npi) == 10:
                add(raw_npi, row)
            elif named and _state_key(row.get(state_col) if state_col else None):
                add(None, row)
            if len(batch) >= 5000:
                con.executemany(
                    """
                    INSERT INTO leie (
                        npi, name, excl_type, excl_date, first_name, last_name, state,
                        middle_name, reinstated_on, origin
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    batch,
                )
                inserted += len(batch)
                batch = []
        if batch:
            con.executemany(
                """
                INSERT INTO leie (
                    npi, name, excl_type, excl_date, first_name, last_name, state,
                    middle_name, reinstated_on, origin
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
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
    with open_text(readable) as handle:
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
        first_name = pick("first_name_owner", "owner_first_name", "first_name")
        middle_name = pick("middle_name_owner", "owner_middle_name", "middle_name")
        last_name = pick("last_name_owner", "owner_last_name", "last_name")
        owner_state = pick("state_owner", "owner_state")
        associated = pick("association_date_owner", "association_date", "effective_date")
        if not associate or not (provider_npi or enrollment):
            raise SystemExit(
                "Ownership file needs an associate id and an NPI or enrollment id. "
                f"Headers: {reader.fieldnames[:24]}"
            )
        con.execute("DELETE FROM ownership_edges WHERE origin = ?", [f"public:{kind}"])
        npi_by_enrollment: dict[str, list[str]] = {}
        for enrollment_id, mapped in con.execute(
            """
            SELECT enrollment_id, npi FROM enrollments
            WHERE npi IS NOT NULL AND npi <> ''
            """
        ).fetchall():
            slot = npi_by_enrollment.setdefault(enrollment_id, [])
            if mapped not in slot:
                slot.append(mapped)
        batch = []
        for row in reader:
            owner_key = (row.get(associate) or "").strip()
            if not owner_key:
                continue
            npi = re.sub(r"\D", "", row.get(provider_npi) or "") if provider_npi else ""
            enrollment_id = (row.get(enrollment) or "").strip() if enrollment else ""
            if len(npi) == 10:
                provider_keys = [npi.zfill(10)]
            elif enrollment_id and npi_by_enrollment.get(enrollment_id):
                provider_keys = list(npi_by_enrollment[enrollment_id])
            elif enrollment_id:
                provider_keys = [enrollment_id]
            else:
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
            person = " ".join(
                part
                for part in (
                    _name_key(row.get(first_name) if first_name else None),
                    _name_key(row.get(last_name) if last_name else None),
                )
                if part
            )
            org = (row.get(org_name) or "").strip() if org_name else ""
            associated_on = _parse_date(row.get(associated) if associated else None)
            for provider_key in provider_keys:
                batch.append(
                    (
                        owner_key,
                        name or person or None,
                        provider_key,
                        percentage,
                        role_code or None,
                        strength,
                        edge_weight,
                        associated_on.isoformat() if associated_on else None,
                        f"{kind}:{enrollment_id}:{owner_key}:{role_code}:{provider_key}",
                        f"public:{kind}",
                        _name_key(row.get(first_name) if first_name else None),
                        _name_key(row.get(middle_name) if middle_name else None),
                        _name_key(row.get(last_name) if last_name else None),
                        _state_key(row.get(owner_state) if owner_state else None),
                    )
                )
                linked_npi = provider_key if len(provider_key) == 10 and provider_key.isdigit() else None
                if org and linked_npi:
                    con.execute(
                        """
                        INSERT INTO providers (
                            provider_key, npi, enrollment_id, ccn, name, entity_type, taxonomy,
                            address, city, state, zip, enumeration_date, origin
                        ) VALUES (?, ?, ?, NULL, ?, 'organization', NULL, NULL, NULL, NULL, NULL, NULL, 'public')
                        ON CONFLICT (provider_key) DO NOTHING
                        """,
                        [provider_key, linked_npi, enrollment_id or None, org or None],
                    )
        if batch:
            con.executemany(
                """
                INSERT INTO ownership_edges (
                    owner_key, owner_name, provider_key, percentage, role_code,
                    role_strength, edge_weight, effective_date, edge_source_id, origin,
                    owner_first, owner_middle, owner_last, owner_state
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    current zip URL. Only 10-digit NPIs are kept. An existing provider row
    keeps its origin.
    """
    settings.ensure_dirs()
    source, locator = _input_path(settings, path, url, "nppes")
    readable = materialize(source, settings.data_dir / "tmp" / "unpack")
    digest, note = store_raw(settings.data_dir, source, "nppes", locator, readable.name)
    if file_kind(readable) == "parquet":
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
        enc = text_encoding(readable)
        with readable.open(newline="", encoding=enc) as handle:
            names = next(csv.reader(handle))
        duck = "cp1252" if enc == "cp1252" else "utf-8"
        reader = (
            "read_csv(?, header=true, sample_size=20000, all_varchar=true, "
            f"ignore_errors=true, encoding='{duck}')"
        )
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


def ingest_enrollments(
    con: duckdb.DuckDBPyConnection,
    settings: Settings,
    path: str | None = None,
    url: str | None = None,
    kind: str = "enrollments",
) -> int:
    """Load PECOS enrollment rows so an enrollment id can be replaced by its NPI."""
    settings.ensure_dirs()
    source, locator = _input_path(settings, path, url, kind)
    readable = materialize(source, settings.data_dir / "tmp" / "unpack")
    digest, note = store_raw(settings.data_dir, source, kind, locator, readable.name)
    inserted = 0
    with open_text(readable) as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise SystemExit("Enrollment file has no header")
        lookup = {normalize_header(name): name for name in reader.fieldnames}

        def pick(*candidates: str) -> str | None:
            for candidate in candidates:
                if candidate in lookup:
                    return lookup[candidate]
            return None

        enrollment = pick("enrollment_id", "enrollment")
        npi_col = pick("npi")
        if not enrollment or not npi_col:
            raise SystemExit(
                "Enrollment file needs ENROLLMENT ID and NPI. "
                f"Headers: {reader.fieldnames[:24]}"
            )
        ccn = pick("ccn")
        org = pick("organization_name", "org_name")
        address = pick("address_line_1", "address")
        city = pick("city")
        state = pick("state")
        postal = pick("zip_code", "zip")
        incorporated = pick("incorporation_date", "enrollment_date")
        origin = f"public:{kind}"
        con.execute("DELETE FROM enrollments WHERE origin = ?", [origin])
        batch = []
        for row in reader:
            enrollment_id = (row.get(enrollment) or "").strip()
            npi = re.sub(r"\D", "", row.get(npi_col) or "")
            if not enrollment_id or len(npi) != 10:
                continue
            formed = _parse_date(row.get(incorporated) if incorporated else None)
            batch.append(
                (
                    enrollment_id,
                    npi,
                    (row.get(ccn) or "").strip() if ccn else None,
                    (row.get(org) or "").strip() if org else None,
                    (row.get(address) or "").strip() if address else None,
                    (row.get(city) or "").strip() if city else None,
                    (row.get(state) or "").strip() if state else None,
                    (row.get(postal) or "").strip() if postal else None,
                    formed,
                    origin,
                )
            )
        if batch:
            con.executemany(
                """
                INSERT INTO enrollments (
                    enrollment_id, npi, ccn, name, address, city, state, zip, incorporation_date, origin
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                batch,
            )
            inserted = len(batch)
            con.executemany(
                """
                INSERT INTO providers (
                    provider_key, npi, enrollment_id, ccn, name, entity_type, taxonomy,
                    address, city, state, zip, enumeration_date, incorporation_date, origin
                ) VALUES (?, ?, ?, ?, ?, 'organization', NULL, ?, ?, ?, ?, NULL, ?, ?)
                ON CONFLICT (provider_key) DO UPDATE SET
                    npi = COALESCE(providers.npi, excluded.npi),
                    enrollment_id = COALESCE(providers.enrollment_id, excluded.enrollment_id),
                    ccn = COALESCE(providers.ccn, excluded.ccn),
                    name = COALESCE(providers.name, excluded.name),
                    address = COALESCE(providers.address, excluded.address),
                    city = COALESCE(providers.city, excluded.city),
                    state = COALESCE(providers.state, excluded.state),
                    zip = COALESCE(providers.zip, excluded.zip),
                    incorporation_date = COALESCE(excluded.incorporation_date, providers.incorporation_date)
                """,
                [
                    (npi, npi, enrollment_id, ccn_value, name, street, city_name, state_name, zip_code, formed, origin)
                    for enrollment_id, npi, ccn_value, name, street, city_name, state_name, zip_code, formed, origin in batch
                ],
            )
    _record_snapshot(con, kind, locator, source, digest, note)
    return inserted


def ingest_medicare_use(
    con: duckdb.DuckDBPyConnection,
    settings: Settings,
    path: str | None = None,
    url: str | None = None,
    setting: str = "hospice",
) -> int:
    """Load one Medicare post-acute provider file, keyed by the CMS certification number."""
    settings.ensure_dirs()
    source, locator = _input_path(settings, path, url, f"medicare-{setting}")
    readable = materialize(source, settings.data_dir / "tmp" / "unpack")
    digest, note = store_raw(settings.data_dir, source, f"medicare-{setting}", locator, readable.name)
    enc = text_encoding(readable)
    duck = "cp1252" if enc == "cp1252" else "utf-8"
    header_con = duckdb.connect()
    names = [
        row[0]
        for row in header_con.execute(
            f"""
            DESCRIBE SELECT * FROM read_csv(
                ?, header=true, sample_size=200, all_varchar=true, ignore_errors=true, encoding='{duck}'
            )
            """,
            [str(readable)],
        ).fetchall()
    ]
    header_con.close()
    lookup = {normalize_header(name): name for name in names}

    def pick(*candidates: str) -> str | None:
        for candidate in candidates:
            if candidate in lookup:
                return lookup[candidate]
        return None

    provider_id = pick("prvdr_id", "ccn", "provider_id")
    state_col = pick("state", "prvdr_state_cd", "provider_state")
    stays_col = pick("tot_epsd_stay_cnt", "stays", "episodes")
    days_col = pick("tot_srvc_days", "service_days")
    if not provider_id or not stays_col or not days_col:
        raise SystemExit(f"Medicare file is missing provider, stay, or day columns. Headers: {names[:24]}")
    year_col = pick("year")
    rhc_col = pick("hospc_rhc_days_pct")
    nursing = pick("nrsng_visits_cnt")
    social = pick("msw_visits_cnt")
    aide = pick("aide_visits_cnt")

    def num(column: str | None) -> str:
        if not column:
            return "CAST(NULL AS DOUBLE)"
        return f'try_cast("{column}" AS DOUBLE)'

    visit_terms = [num(column) for column in (nursing, social, aide) if column]
    visits = " + ".join(f"COALESCE({term}, 0)" for term in visit_terms) if visit_terms else "CAST(NULL AS DOUBLE)"
    year_sql = f'try_cast("{year_col}" AS INTEGER)' if year_col else "CAST(NULL AS INTEGER)"
    state_sql = f'NULLIF(upper(trim(CAST("{state_col}" AS VARCHAR))), \'\')' if state_col else "CAST(NULL AS VARCHAR)"
    origin = f"public:medicare-{setting}"
    con.execute("DELETE FROM medicare_use WHERE origin = ?", [origin])
    con.execute(
        f"""
        INSERT INTO medicare_use (ccn, setting, year, stays, service_days, visits, rhc_days_pct, state, origin)
        SELECT
            regexp_replace(CAST("{provider_id}" AS VARCHAR), '[^0-9]', '', 'g'),
            ?,
            {year_sql},
            {num(stays_col)},
            {num(days_col)},
            {visits},
            {num(rhc_col)},
            {state_sql},
            ?
        FROM read_csv(?, header=true, all_varchar=true, ignore_errors=true, encoding='{duck}')
        WHERE regexp_full_match(regexp_replace(CAST("{provider_id}" AS VARCHAR), '[^0-9]', '', 'g'), '[0-9]+')
        """,
        [setting, origin, str(readable)],
    )
    count = con.execute("SELECT COUNT(*) FROM medicare_use WHERE origin = ?", [origin]).fetchone()[0]
    _record_snapshot(con, f"medicare-{setting}", locator, source, digest, note)
    return int(count)


def ingest_care_compare(
    con: duckdb.DuckDBPyConnection,
    settings: Settings,
    path: str | None = None,
    url: str | None = None,
    setting: str = "hospice",
) -> int:
    """Load a CMS Care Compare provider file.

    Hospice files are one row per measure. Home health files are one row per agency.
    """
    settings.ensure_dirs()
    source, locator = _input_path(settings, path, url, f"care-{setting}")
    readable = materialize(source, settings.data_dir / "tmp" / "unpack")
    digest, note = store_raw(settings.data_dir, source, f"care-{setting}", locator, readable.name)
    enc = text_encoding(readable)
    duck = "cp1252" if enc == "cp1252" else "utf-8"
    header_con = duckdb.connect()
    names = [
        row[0]
        for row in header_con.execute(
            f"""
            DESCRIBE SELECT * FROM read_csv(
                ?, header=true, sample_size=200, all_varchar=true, ignore_errors=true, encoding='{duck}'
            )
            """,
            [str(readable)],
        ).fetchall()
    ]
    header_con.close()
    lookup = {normalize_header(name): name for name in names}

    def pick(*candidates: str) -> str | None:
        for candidate in candidates:
            if candidate in lookup:
                return lookup[candidate]
        return None

    ccn_col = pick("cms_certification_number_ccn", "cms_certification_number", "ccn")
    state_col = pick("state")
    if not ccn_col:
        raise SystemExit(f"Care Compare file has no certification number. Headers: {names[:12]}")
    origin = f"public:care-{setting}"
    con.execute("DELETE FROM care_compare WHERE origin = ?", [origin])
    reader = f"read_csv(?, header=true, all_varchar=true, ignore_errors=true, encoding='{duck}')"
    state_sql = f'NULLIF(upper(trim("{state_col}")), \'\')' if state_col else "CAST(NULL AS VARCHAR)"
    ccn_sql = f'regexp_replace(CAST("{ccn_col}" AS VARCHAR), \'[^0-9]\', \'\', \'g\')'
    measure_col = pick("measure_name")
    score_col = pick("score")
    if measure_col and score_col:
        con.execute(
            f"""
            INSERT INTO care_compare (ccn, state, setting, measure, score, origin)
            SELECT {ccn_sql}, {state_sql}, ?,
                   CASE
                     WHEN "{measure_col}" ILIKE 'Early live discharges%' THEN 'Early live discharges'
                     WHEN "{measure_col}" ILIKE 'Late live discharges%' THEN 'Late live discharges'
                   END,
                   try_cast("{score_col}" AS DOUBLE),
                   ?
            FROM {reader}
            WHERE "{measure_col}" ILIKE 'Early live discharges%'
               OR "{measure_col}" ILIKE 'Late live discharges%'
            """,
            [setting, origin, str(readable)],
        )
    else:
        wide = pick("ppr_risk_standardized_rate")
        if not wide:
            raise SystemExit(f"Care Compare file has no measure column. Headers: {names[:12]}")
        con.execute(
            f"""
            INSERT INTO care_compare (ccn, state, setting, measure, score, origin)
            SELECT {ccn_sql}, {state_sql}, ?, 'PPR Risk-Standardized Rate',
                   try_cast("{wide}" AS DOUBLE), ?
            FROM {reader}
            WHERE try_cast("{wide}" AS DOUBLE) IS NOT NULL
            """,
            [setting, origin, str(readable)],
        )
    count = con.execute(
        "SELECT COUNT(*) FROM care_compare WHERE origin = ? AND score IS NOT NULL AND ccn <> ''",
        [origin],
    ).fetchone()[0]
    con.execute("DELETE FROM care_compare WHERE origin = ? AND (score IS NULL OR ccn = '')", [origin])
    _record_snapshot(con, f"care-{setting}", locator, source, digest, note)
    return int(count)
