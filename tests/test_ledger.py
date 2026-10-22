from __future__ import annotations

import tempfile
import unittest
from argparse import Namespace
from datetime import date
from pathlib import Path

from fraudledger.columns import resolve_columns
from fraudledger.db import connect
from fraudledger.graph.cluster import build_communities
from fraudledger.graph.edges import qualifies, weight
from fraudledger.ingest.demo import load_demo
from fraudledger.ingest.public import ingest_medicaid, ingest_nppes, ingest_ownership
from fraudledger.pipeline import execute_run
from fraudledger.report.assemble import BANNED, verify_run
from fraudledger.settings import load_settings


def flags_for(con, run_id: str, npi: str) -> set[str]:
    return {
        row[0]
        for row in con.execute(
            "SELECT rule_id FROM flags WHERE run_id = ? AND provider_key = ?",
            [run_id, npi],
        ).fetchall()
    }


class RuleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.settings = load_settings(data_dir=self.tmp.name)
        self.settings.ensure_dirs()
        self.con = connect(self.settings.db_path)
        load_demo(self.con, self.settings)
        self.run_id = execute_run(self.con, self.settings, run_id="TEST")

    def tearDown(self) -> None:
        self.con.close()
        self.tmp.cleanup()

    def assert_has(self, npi: str, rule_id: str) -> None:
        found = flags_for(self.con, self.run_id, npi)
        self.assertIn(rule_id, found, msg=f"{npi} flags were {sorted(found)}")

    def test_clean_control_has_no_flags(self) -> None:
        self.assertEqual(flags_for(self.con, self.run_id, "1999990004"), set())

    def test_intended_rules(self) -> None:
        self.assert_has("1999990001", "R01")
        found = flags_for(self.con, self.run_id, "1999990002")
        self.assertIn("R06", found, msg=sorted(found))
        self.assertNotIn("R01", found)
        self.assertNotIn("R11", found)
        found = flags_for(self.con, self.run_id, "1999990003")
        self.assertEqual(found, {"R11"})
        self.assert_has("1999990008", "R08")
        self.assert_has("1999990009", "R03")
        self.assert_has("1999990005", "R09")
        self.assert_has("1999990013", "R10")

    def test_cluster_members_share_a_community(self) -> None:
        rows = self.con.execute(
            "SELECT cluster_id, provider_key FROM cluster_members WHERE run_id = ?",
            [self.run_id],
        ).fetchall()
        groups: dict[int, set[str]] = {}
        for cluster_id, key in rows:
            groups.setdefault(cluster_id, set()).add(key)
        shared = None
        for members in groups.values():
            if "1999990010" in members and "1999990011" in members:
                shared = members
        self.assertIsNotNone(shared)
        self.assertIn("1999990012", shared)
        self.assertNotIn("1999990004", shared)
        self.assertNotIn("1999990001", shared)

    def test_language_and_manifest(self) -> None:
        run_dir = self.settings.data_dir / "runs" / self.run_id
        text = (run_dir / "report.txt").read_text(encoding="utf-8").lower()
        html = (run_dir / "report.html").read_text(encoding="utf-8").lower()
        for phrase in BANNED:
            self.assertNotIn(phrase, text)
            self.assertNotIn(phrase, html)
        verify_run(run_dir)
        evidence = next((run_dir / "evidence").glob("*.csv"))
        original = evidence.read_bytes()
        flipped = bytearray(original)
        flipped[0] = (flipped[0] + 1) % 256
        evidence.write_bytes(flipped)
        with self.assertRaises(SystemExit):
            verify_run(run_dir)
        evidence.write_bytes(original)


class GraphTests(unittest.TestCase):
    def test_weight_and_qualifying_roles(self) -> None:
        settings = load_settings(data_dir="/tmp")
        self.assertTrue(qualifies(5, None, settings))
        self.assertFalse(qualifies(4, "99", settings))
        self.assertTrue(qualifies(1, "40", settings))
        self.assertAlmostEqual(weight(100, "40", settings), 3.0)

    def test_disconnected_owners_stay_apart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = load_settings(data_dir=tmp)
            settings.ensure_dirs()
            con = connect(settings.db_path)
            con.execute(
                """
                INSERT INTO ownership_edges (
                    owner_key, owner_name, provider_key, percentage, role_code,
                    role_strength, edge_weight, effective_date, edge_source_id, origin
                ) VALUES
                ('O1', 'One', 'P1', 100, '40', 3, 3, NULL, 'a', 'test'),
                ('O1', 'One', 'P2', 100, '40', 3, 3, NULL, 'b', 'test'),
                ('O2', 'Two', 'P3', 100, '40', 3, 3, NULL, 'c', 'test'),
                ('O2', 'Two', 'P4', 100, '40', 3, 3, NULL, 'd', 'test')
                """
            )
            build_communities(con, settings, "GRAPH")
            rows = con.execute(
                "SELECT cluster_id, provider_key FROM cluster_members WHERE run_id = 'GRAPH'"
            ).fetchall()
            groups: dict[int, set[str]] = {}
            for cluster_id, key in rows:
                groups.setdefault(cluster_id, set()).add(key)
            sets = {frozenset(members) for members in groups.values()}
            self.assertIn(frozenset({"P1", "P2"}), sets)
            self.assertIn(frozenset({"P3", "P4"}), sets)
            con.close()


class IngestTests(unittest.TestCase):
    def test_cms_headers(self) -> None:
        mapping = resolve_columns(
            [
                "BILLING_PROVIDER_NPI_NUM",
                "SERVICING_PROVIDER_NPI_NUM",
                "HCPCS_CODE",
                "CLAIM_FROM_MONTH",
                "TOTAL_UNIQUE_BENEFICIARIES",
                "TOTAL_CLAIMS",
                "TOTAL_PAID",
            ]
        )
        self.assertEqual(mapping["billing_npi"], "BILLING_PROVIDER_NPI_NUM")
        self.assertEqual(mapping["claim_lines"], "TOTAL_CLAIMS")
        patients = resolve_columns(
            [
                "BILLING_PROVIDER_NPI_NUM",
                "HCPCS_CODE",
                "CLAIM_FROM_MONTH",
                "TOTAL_PATIENTS",
                "TOTAL_CLAIM_LINES",
                "TOTAL_PAID",
            ]
        )
        self.assertEqual(patients["beneficiaries"], "TOTAL_PATIENTS")

    def test_nppes_does_not_rebrand_demo_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = load_settings(data_dir=tmp)
            settings.ensure_dirs()
            con = connect(settings.db_path)
            con.execute(
                """
                INSERT INTO providers (
                    provider_key, npi, enrollment_id, ccn, name, entity_type, taxonomy,
                    address, city, state, zip, enumeration_date, origin
                ) VALUES ('1999990001', '1999990001', NULL, NULL, 'Demo Name', 'organization', NULL,
                          NULL, NULL, NULL, NULL, NULL, 'demo')
                """
            )
            path = Path(tmp) / "nppes.csv"
            path.write_text(
                "NPI,Provider Organization Name (Legal Business Name),Provider Enumeration Date,"
                "Provider First Line Business Practice Location Address,"
                "Provider Business Practice Location Address City Name,"
                "Provider Business Practice Location Address State Name,"
                "Healthcare Provider Taxonomy Code_1,Entity Type Code\n"
                "1999990001,Public Name,01/15/2024,1 Main,Austin,TX,251E00000X,2\n"
                "1234567890,Other Org,2020-02-02,2 Main,Austin,TX,251E00000X,2\n",
                encoding="utf-8",
            )
            count = ingest_nppes(con, settings, path=str(path))
            self.assertEqual(count, 2)
            origin, name, enumerated = con.execute(
                "SELECT origin, name, enumeration_date FROM providers WHERE provider_key = '1999990001'"
            ).fetchone()
            self.assertEqual(origin, "demo")
            self.assertEqual(enumerated, date(2024, 1, 15))
            self.assertTrue(name)
            con.close()

    def test_ownership_keeps_control_roles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = load_settings(data_dir=tmp)
            settings.ensure_dirs()
            con = connect(settings.db_path)
            path = Path(tmp) / "owners.csv"
            path.write_text(
                "ASSOCIATE ID - OWNER,ORGANIZATION NAME - OWNER,NPI,ROLE CODE - OWNER,PERCENTAGE OWNERSHIP\n"
                "A1,Kept,1999990010,40,100\n"
                "A2,Dropped,1999990011,99,1\n",
                encoding="utf-8",
            )
            kept = ingest_ownership(con, settings, path=str(path), kind="hospice_owners")
            self.assertEqual(kept, 1)
            row = con.execute("SELECT provider_key, origin FROM ownership_edges").fetchone()
            self.assertEqual(row, ("1999990010", "public:hospice_owners"))
            con.close()

    def test_refresh_without_urls_is_a_noop(self) -> None:
        from fraudledger.cli import _cmd_refresh

        with tempfile.TemporaryDirectory() as tmp:
            code = _cmd_refresh(Namespace(config=None, data_dir=tmp))
            self.assertEqual(code, 0)


class ApiTests(unittest.TestCase):
    def test_health_and_queue(self) -> None:
        from fastapi.testclient import TestClient

        from fraudledger.api.app import create_app

        with tempfile.TemporaryDirectory() as tmp:
            settings = load_settings(data_dir=tmp)
            settings.ensure_dirs()
            con = connect(settings.db_path)
            load_demo(con, settings)
            execute_run(con, settings, run_id="API")
            con.close()
            client = TestClient(create_app(settings))
            health = client.get("/health")
            self.assertEqual(health.status_code, 200)
            self.assertEqual(health.json()["status"], "ok")
            page = client.get("/")
            self.assertEqual(page.status_code, 200)
            self.assertIn("1999990001", page.text)
            self.assertIn("Demonstration data", page.text)
            self.assertIn("demonstration data", page.text.lower())
            denied = client.get("/report/1999990001?run=API")
            self.assertEqual(denied.status_code, 409)
            removed = client.post("/demo/remove", follow_redirects=False)
            self.assertEqual(removed.status_code, 303)
            cleared = client.get("/")
            self.assertNotIn("1999990001", cleared.text)
            self.assertNotIn("Demonstration data", cleared.text)
            self.assertNotIn("fraudster", page.text.lower())
            missing = client.get("/provider/not a key")
            self.assertEqual(missing.status_code, 400)

    def test_sources_page_and_rejected_url(self) -> None:
        from fastapi.testclient import TestClient

        from fraudledger.api.app import create_app

        with tempfile.TemporaryDirectory() as tmp:
            settings = load_settings(data_dir=tmp)
            client = TestClient(create_app(settings))
            page = client.get("/sources")
            self.assertEqual(page.status_code, 200)
            self.assertIn("Your own URL", page.text)
            self.assertIn("Medicaid Provider Spending", page.text)
            self.assertIn("Public working set", page.text)
            status = Path(tmp) / "logs" / "pull_status.json"
            status.parent.mkdir(parents=True, exist_ok=True)
            status.write_text(
                '{"running": true, "message": "OIG exclusion list", "error": "", "percent": 25, "received": 2500000, "total": 10000000}',
                encoding="utf-8",
            )
            loading = client.get("/sources")
            self.assertIn("25%", loading.text)
            self.assertIn('role="progressbar"', loading.text)
            self.assertIn("OIG exclusion list", loading.text)
            status.write_text(
                '{"running": false, "message": "", "error": "", "percent": null}',
                encoding="utf-8",
            )
            rejected = client.post(
                "/sources",
                data={"mode": "url", "url": "ftp://example.com/file.csv", "as_kind": "leie"},
            )
            self.assertEqual(rejected.status_code, 400)

    def test_queue_answers_while_the_database_lock_is_held(self) -> None:
        import threading
        import time

        from fastapi.testclient import TestClient

        from fraudledger.api.app import _LOCK, create_app

        with tempfile.TemporaryDirectory() as tmp:
            settings = load_settings(data_dir=tmp)
            release = threading.Event()
            started = threading.Event()

            def hold() -> None:
                with _LOCK:
                    started.set()
                    release.wait(5)

            thread = threading.Thread(target=hold)
            thread.start()
            self.assertTrue(started.wait(2))
            try:
                client = TestClient(create_app(settings))
                began = time.monotonic()
                page = client.get("/")
                elapsed = time.monotonic() - began
            finally:
                release.set()
                thread.join(5)
            self.assertLess(elapsed, 3)
            self.assertEqual(page.status_code, 200)
            self.assertIn("database", page.text.lower())
            self.assertNotIn("fraudster", page.text.lower())
            broken = Path(tmp) / "parquet" / "facts" / "broken.parquet"
            broken.parent.mkdir(parents=True, exist_ok=True)
            broken.write_bytes(b"PAR1 this is not a parquet file")
            again = client.get("/")
            self.assertEqual(again.status_code, 200)
            self.assertIn("Review queue", again.text)
            self.assertFalse(broken.exists())


class ReaderIndexTests(unittest.TestCase):
    def test_index_explains_score_and_role_codes(self) -> None:
        from fastapi.testclient import TestClient

        from fraudledger.api.app import create_app
        from fraudledger.reference import reading_notes
        from fraudledger.report.assemble import BANNED

        with tempfile.TemporaryDirectory() as tmp:
            settings = load_settings(data_dir=tmp)
            client = TestClient(create_app(settings))
            page = client.get("/index")
            self.assertEqual(page.status_code, 200)
            self.assertIn("5% or greater direct ownership interest", page.text)
            self.assertIn("Do not add it to role 34", page.text)
            self.assertIn("How to read", page.text)
            self.assertIn("role strength", page.text)
            notes = reading_notes(settings)
            self.assertIn("Add role 34 once per owner", notes)
            self.assertIn("at least 8", notes)
            lowered = (page.text + notes).lower()
            for phrase in BANNED:
                self.assertNotIn(phrase, lowered)


class CatalogTests(unittest.TestCase):
    def test_medicaid_prefers_latest_parquet(self) -> None:
        from fraudledger.ingest.catalog import medicaid_url_from_html

        html = """
        <a href="https://example.test/datasets/medicaid-provider-spending/2026-02-09/dataset/medicaid-provider-spending.csv.zip">zip</a>
        <a href="https://example.test/datasets/medicaid-provider-spending/2026-02-09/dataset/medicaid-provider-spending.parquet">parquet</a>
        <a href="https://example.test/datasets/medicaid-provider-spending/2024-01-01/dataset/medicaid-provider-spending.parquet">old</a>
        """
        chosen = medicaid_url_from_html(html)
        self.assertTrue(chosen.endswith("2026-02-09/dataset/medicaid-provider-spending.parquet"))

    def test_nppes_skips_weekly_and_reports_maintenance(self) -> None:
        from fraudledger.ingest.catalog import nppes_monthly_url_from_html

        html = """
        <a href="NPPES_Data_Dissemination_October_2026.zip">NPPES Data Dissemination V.2 (October 12, 2026)</a>
        <a href="NPPES_Deactivated_NPI_Report.zip">Monthly Deactivation Update</a>
        <a href="weekly/NPPES_Weekly.zip">Weekly Update</a>
        """
        self.assertEqual(
            nppes_monthly_url_from_html(html),
            "https://download.cms.gov/nppes/NPPES_Data_Dissemination_October_2026.zip",
        )
        with self.assertRaises(SystemExit):
            nppes_monthly_url_from_html("<h2>This site will be down for maintenance until further notice</h2>")

    def test_cms_catalog_picks_newest_csv(self) -> None:
        from fraudledger.ingest.catalog import latest_csv_url, validate_http_url

        catalog = {
            "dataset": [
                {
                    "title": "Hospice All Owners : 2026-04-01",
                    "modified": "2026-06-17",
                    "distribution": [{"downloadURL": "https://data.cms.gov/old.csv", "format": "CSV"}],
                },
                {
                    "title": "Hospice All Owners : 2026-10-01",
                    "modified": "2026-10-19",
                    "distribution": [{"downloadURL": "https://data.cms.gov/new.csv", "format": "CSV"}],
                },
                {
                    "title": "Hospice Enrollments : 2026-10-01",
                    "modified": "2026-10-19",
                    "distribution": [{"downloadURL": "https://data.cms.gov/enroll.csv", "format": "CSV"}],
                },
            ]
        }
        self.assertEqual(latest_csv_url(catalog, "Hospice All Owners"), "https://data.cms.gov/new.csv")
        with self.assertRaises(SystemExit):
            validate_http_url("file:///tmp/secret.csv")

    def test_enrollment_npi_links_owner_edge(self) -> None:
        from fraudledger.ingest.public import ingest_enrollments, ingest_ownership

        with tempfile.TemporaryDirectory() as tmp:
            settings = load_settings(data_dir=tmp)
            settings.ensure_dirs()
            con = connect(settings.db_path)
            enrollments = Path(tmp) / "enroll.csv"
            enrollments.write_text(
                "ENROLLMENT ID,NPI,ORGANIZATION NAME,ADDRESS LINE 1,CITY,STATE,ZIP CODE\n"
                "ENR1,1999990010,Linked Hospice,10 SHARED WAY,PHOENIX,AZ,85001\n"
                "ENR1,1999990011,Linked Hospice,10 SHARED WAY,PHOENIX,AZ,85001\n",
                encoding="utf-8",
            )
            owners = Path(tmp) / "owners.csv"
            owners.write_text(
                "ENROLLMENT ID,ASSOCIATE ID - OWNER,ORGANIZATION NAME - OWNER,ROLE CODE - OWNER,PERCENTAGE OWNERSHIP\n"
                "ENR1,A100,Shared Control LLC,40,100\n",
                encoding="utf-8",
            )
            self.assertEqual(ingest_enrollments(con, settings, path=str(enrollments), kind="hospice-enrollments"), 2)
            kept = ingest_ownership(con, settings, path=str(owners), kind="hospice-owners")
            self.assertEqual(kept, 2)
            keys = {
                row[0]
                for row in con.execute("SELECT provider_key FROM ownership_edges ORDER BY 1").fetchall()
            }
            self.assertEqual(keys, {"1999990010", "1999990011"})
            con.close()


class PublicReportTests(unittest.TestCase):
    def test_priority_focus_and_report_packet(self) -> None:
        import zipfile

        from fraudledger.ingest.public import ingest_medicaid
        from fraudledger.report.packet import build_packet

        with tempfile.TemporaryDirectory() as tmp:
            settings = load_settings(data_dir=tmp)
            settings.ensure_dirs()
            con = connect(settings.db_path)
            load_demo(con, settings)
            execute_run(con, settings, run_id="DEMO")
            with self.assertRaises(SystemExit) as caught:
                build_packet(con, settings, "DEMO", "1999990001")
            self.assertIn("demonstration", str(caught.exception).lower())

            path = Path(tmp) / "spend.csv"
            path.write_text(
                "BILLING_PROVIDER_NPI_NUM,HCPCS_CODE,CLAIM_FROM_MONTH,TOTAL_PAID,TOTAL_CLAIM_LINES,TOTAL_UNIQUE_BENEFICIARIES\n"
                "1234567893,T2042,2024-01-01,100,2,1\n"
                "1234567893,99213,2024-01-01,50,1,1\n",
                encoding="utf-8",
            )
            ingest_medicaid(con, settings, path=str(path), drop_demo=True, focus="priority")
            codes = {row[0] for row in con.execute("SELECT hcpcs FROM facts_medicaid").fetchall()}
            self.assertEqual(codes, {"T2042"})
            execute_run(con, settings, run_id="PUBLIC")
            dest = build_packet(con, settings, "PUBLIC", "1234567893")
            with zipfile.ZipFile(dest) as archive:
                names = set(archive.namelist())
                narrative = archive.read("narrative.txt").decode("utf-8")
                submit = archive.read("how-to-submit.txt").decode("utf-8")
            self.assertEqual(
                names,
                {"narrative.txt", "evidence.csv", "indicators.csv", "how-to-submit.txt", "report.pdf"},
            )
            self.assertIn("1234567893", narrative)
            self.assertIn("https://america.gov/fraud", narrative)
            self.assertIn("does not contact", submit.lower())
            blob = (narrative + submit).lower()
            for phrase in BANNED:
                self.assertNotIn(phrase, blob)
            con.close()

    def test_purge_keeps_a_public_row(self) -> None:
        from fraudledger.ingest.demo import purge_demo
        from fraudledger.ingest.public import ingest_medicaid

        with tempfile.TemporaryDirectory() as tmp:
            settings = load_settings(data_dir=tmp)
            settings.ensure_dirs()
            con = connect(settings.db_path)
            load_demo(con, settings)
            execute_run(con, settings, run_id="DEMO")
            path = Path(tmp) / "spend.csv"
            path.write_text(
                "BILLING_PROVIDER_NPI_NUM,HCPCS_CODE,CLAIM_FROM_MONTH,TOTAL_PAID,TOTAL_CLAIM_LINES\n"
                "1234567893,T2042,2024-01-01,100,2\n",
                encoding="utf-8",
            )
            ingest_medicaid(con, settings, path=str(path))
            message = purge_demo(con, settings)
            self.assertIn("Public rows were not deleted", message)
            self.assertFalse((Path(tmp) / "parquet" / "facts" / "demo.parquet").exists())
            self.assertEqual(
                con.execute("SELECT COUNT(*) FROM providers WHERE provider_key LIKE '199999%'").fetchone()[0],
                0,
            )
            self.assertEqual(
                con.execute("SELECT COUNT(*) FROM runs WHERE demo_only = TRUE").fetchone()[0],
                0,
            )
            self.assertEqual(
                {row[0] for row in con.execute("SELECT hcpcs FROM facts_medicaid").fetchall()},
                {"T2042"},
            )
            con.close()

    def test_windows1252_enrollment_name(self) -> None:
        from fraudledger.ingest.common import text_encoding
        from fraudledger.ingest.public import ingest_enrollments

        with tempfile.TemporaryDirectory() as tmp:
            settings = load_settings(data_dir=tmp)
            settings.ensure_dirs()
            con = connect(settings.db_path)
            path = Path(tmp) / "enroll.csv"
            path.write_bytes(
                b"ENROLLMENT ID,NPI,ORGANIZATION NAME\r\n"
                b"ENR9,1234567893,HOSPICE \xbf ST GEORGE\r\n"
            )
            self.assertEqual(text_encoding(path), "cp1252")
            ingest_enrollments(con, settings, path=str(path), kind="hospice-enrollments")
            name = con.execute(
                "SELECT name FROM providers WHERE provider_key = '1234567893'"
            ).fetchone()[0]
            self.assertIn("HOSPICE", name)
            self.assertIn("ST GEORGE", name)
            con.close()

    def test_parquet_download_is_not_read_as_text(self) -> None:
        import duckdb

        from fraudledger.ingest.common import file_kind, materialize, open_text
        from fraudledger.ingest.public import ingest_medicaid

        with tempfile.TemporaryDirectory() as tmp:
            settings = load_settings(data_dir=tmp)
            settings.ensure_dirs()
            saved = Path(tmp) / "medicaid.download"
            duckdb.connect().execute(
                """
                COPY (
                    SELECT
                        '1234567893' AS billing_provider_npi_num,
                        'T2042' AS hcpcs_code,
                        DATE '2024-01-01' AS claim_from_month,
                        10.0 AS total_paid,
                        2.0 AS total_claim_lines
                ) TO ? (FORMAT PARQUET)
                """,
                [str(saved)],
            )
            raw = saved.read_bytes()
            self.assertTrue(raw.startswith(b"PAR1"))
            self.assertEqual(file_kind(saved), "parquet")
            con = connect(settings.db_path)
            count = ingest_medicaid(con, settings, path=str(saved))
            self.assertGreaterEqual(count, 1)
            self.assertEqual(
                con.execute("SELECT hcpcs FROM facts_medicaid").fetchone()[0],
                "T2042",
            )
            con.close()

            odd = Path(tmp) / "odd.csv"
            odd.write_bytes(b"name\r\n" + b" " * 37 + b"\x90\r\n")
            with open_text(odd) as handle:
                handle.read()

            zipped = Path(tmp) / "nppes.download"
            import zipfile

            with zipfile.ZipFile(zipped, "w") as archive:
                archive.writestr("npidata.csv", "NPI\n1234567893\n")
            extracted = materialize(zipped, Path(tmp) / "unpack")
            self.assertEqual(extracted.suffix, ".csv")
            self.assertIn("1234567893", extracted.read_text(encoding="utf-8"))

    def test_claim_month_may_be_year_and_month(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = load_settings(data_dir=tmp)
            settings.ensure_dirs()
            path = Path(tmp) / "month.csv"
            path.write_text(
                "BILLING_PROVIDER_NPI_NUM,HCPCS_CODE,CLAIM_FROM_MONTH,TOTAL_PAID,TOTAL_CLAIM_LINES\n"
                "1234567893,T2042,2024-04,80,4\n"
                "1234567893,T2042,not-a-month,9,1\n",
                encoding="utf-8",
            )
            con = connect(settings.db_path)
            ingest_medicaid(con, settings, path=str(path), focus="priority")
            rows = con.execute("SELECT month, paid FROM facts_medicaid ORDER BY month").fetchall()
            con.close()
            self.assertEqual(rows, [(date(2024, 4, 1), 80.0)])

    def test_zero_prior_year_does_not_stop_scoring(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = load_settings(data_dir=tmp)
            settings.ensure_dirs()
            path = Path(tmp) / "yoy.csv"
            path.write_text(
                "BILLING_PROVIDER_NPI_NUM,HCPCS_CODE,CLAIM_FROM_MONTH,TOTAL_PAID,TOTAL_CLAIM_LINES\n"
                "1234567893,T2042,2023-01,0,1\n"
                "1234567893,T2042,2024-04,2000000,10\n",
                encoding="utf-8",
            )
            con = connect(settings.db_path)
            ingest_medicaid(con, settings, path=str(path), focus="priority")
            run_id = execute_run(con, settings, run_id="ZERO")
            flags = flags_for(con, run_id, "1234567893")
            con.close()
            self.assertNotIn("R07", flags)
            self.assertNotIn("R08", flags)

    def test_missing_provider_name_does_not_stop_the_report(self) -> None:
        from fraudledger.report.assemble import write_report

        with tempfile.TemporaryDirectory() as tmp:
            settings = load_settings(data_dir=tmp)
            settings.ensure_dirs()
            path = Path(tmp) / "named.csv"
            path.write_text(
                "BILLING_PROVIDER_NPI_NUM,HCPCS_CODE,CLAIM_FROM_MONTH,TOTAL_PAID,TOTAL_CLAIM_LINES\n"
                ",T2042,2024-04,9000000,20\n"
                "1234567893,T2042,2024-04,15,1\n",
                encoding="utf-8",
            )
            con = connect(settings.db_path)
            ingest_medicaid(con, settings, path=str(path), focus="priority")
            blank = con.execute(
                "SELECT COUNT(*) FROM facts_medicaid WHERE billing_npi IS NULL OR trim(billing_npi) = ''"
            ).fetchone()[0]
            run_id = execute_run(con, settings, run_id="NAME")
            con.execute(
                """
                INSERT INTO scores (run_id, provider_key, score, tier, flag_count, excess_dollars, rules)
                VALUES (?, NULL, 20, 'High', 1, 0, 'R05')
                """,
                [run_id],
            )
            report = write_report(con, settings, run_id)
            con.close()
            self.assertEqual(blank, 0)
            self.assertTrue((report / "report.txt").is_file())
            self.assertNotIn("NoneType", (report / "report.txt").read_text(encoding="utf-8"))


class StrengthTests(unittest.TestCase):
    def test_added_public_fields_change_the_indicators(self) -> None:
        from fraudledger.detect.engine import apply_rules
        from fraudledger.ingest.public import ingest_medicare_use

        with tempfile.TemporaryDirectory() as tmp:
            settings = load_settings(data_dir=tmp)
            settings.ensure_dirs()
            path = Path(tmp) / "facts.csv"
            lines = ["BILLING_PROVIDER_NPI_NUM,HCPCS_CODE,CLAIM_FROM_MONTH,TOTAL_PATIENTS,TOTAL_PAID,TOTAL_CLAIM_LINES"]
            lines.append("1234567893,T1019,2023-01,100,1000000,10")
            lines.append("1234567893,T1019,2024-01,400,4000000,10")
            lines.append("1234567894,T1019,2023-01,100,1000000,10")
            lines.append("1234567894,T1019,2024-01,100,4000000,10")
            lines.append("1234567895,T2042,2024-01,20,2000000,10")
            lines.append("1234567896,T2042,2024-01,20,2000000,10")
            lines.append("1234567897,T1019,2024-06,40,6000000,20")
            lines.append("1234567898,T1019,2024-06,40,6000000,20")
            for index in range(8):
                npi = f"12345678{index:02d}"[-10:]
                lines.append(f"{npi},T2042,2024-03,10,1000,1")
            path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            con = connect(settings.db_path)
            ingest_medicaid(con, settings, path=str(path), focus="priority")
            con.executemany(
                """
                INSERT INTO providers (
                    provider_key, npi, name, entity_type, taxonomy, enumeration_date, incorporation_date, origin
                ) VALUES (?, ?, 'Example', 'organization', ?, ?, ?, 'public')
                """,
                [
                    ("1234567893", "1234567893", "251E00000X", date(2018, 1, 1), None),
                    ("1234567894", "1234567894", "251E00000X", date(2018, 1, 1), None),
                    ("1234567895", "1234567895", "251G00000X", date(2016, 1, 1), None),
                    ("1234567896", "1234567896", "251E00000X", date(2016, 1, 1), None),
                    ("1234567897", "1234567897", "251E00000X", date(2023, 6, 1), date(2010, 1, 1)),
                    ("1234567898", "1234567898", "251E00000X", date(2023, 6, 1), None),
                ],
            )
            con.execute(
                """
                INSERT INTO ownership_edges (
                    owner_key, owner_name, provider_key, percentage, role_code, role_strength,
                    edge_weight, effective_date, edge_source_id, origin, owner_first, owner_last, owner_state
                ) VALUES
                    ('O1', 'JANE SMITH', '1234567894', 100, '40', 3, 3, '2024-02-01', 'e1', 'public', 'JANE', 'SMITH', 'TX'),
                    ('O2', 'JANE SMITH', '1234567893', 100, '40', 3, 3, '2024-02-01', 'e2', 'public', 'JANE', 'SMITH', 'TX')
                """
            )
            con.execute(
                """
                INSERT INTO leie (npi, name, excl_type, excl_date, first_name, last_name, state, origin)
                VALUES (NULL, 'JANE SMITH', '1128a1', DATE '2020-01-01', 'JANE', 'SMITH', 'TX', 'public')
                """
            )
            con.executemany(
                """
                INSERT INTO enrollments (enrollment_id, npi, ccn, origin)
                VALUES (?, ?, ?, 'public')
                """,
                [(f"E{index}", f"12345678{index:02d}"[-10:], f"{index + 1:06d}") for index in range(8)],
            )
            medicare = Path(tmp) / "hospice.csv"
            medicare_rows = ["YEAR,PRVDR_ID,SMRY_CTGRY,TOT_EPSD_STAY_CNT,TOT_SRVC_DAYS,HOSPC_RHC_DAYS_PCT"]
            medicare_rows.append("2024,NATIONAL TOTAL,NATION,100,1000,90")
            for index in range(8):
                days = 20000 if index == 0 else 5000
                medicare_rows.append(f"2024,{index + 1:06d},PROVIDER,100,{days},98")
            medicare.write_text("\n".join(medicare_rows) + "\n", encoding="utf-8")
            ingest_medicare_use(con, settings, path=str(medicare), setting="hospice")
            apply_rules(con, settings, "STRENGTH")
            tracked = flags_for(con, "STRENGTH", "1234567893")
            flat_census = flags_for(con, "STRENGTH", "1234567894")
            hospice = flags_for(con, "STRENGTH", "1234567895")
            home_health = flags_for(con, "STRENGTH", "1234567896")
            old_org = flags_for(con, "STRENGTH", "1234567897")
            new_npi = flags_for(con, "STRENGTH", "1234567898")
            long_stay = flags_for(con, "STRENGTH", "1234567800")
            ordinary = flags_for(con, "STRENGTH", "1234567801")
            con.close()
            self.assertNotIn("R07", tracked)
            self.assertIn("R07", flat_census)
            self.assertIn("R12", flat_census)
            self.assertIn("R13", flat_census)
            self.assertNotIn("R05", hospice)
            self.assertIn("R05", home_health)
            self.assertNotIn("R10", old_org)
            self.assertIn("R10", new_npi)
            self.assertIn("R14", long_stay)
            self.assertNotIn("R14", ordinary)


if __name__ == "__main__":
    unittest.main()
