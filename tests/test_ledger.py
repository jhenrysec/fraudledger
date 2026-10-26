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
from tests.fixture import load_demo
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
            path = Path(tmp) / "spend.csv"
            path.write_text(
                "BILLING_PROVIDER_NPI_NUM,HCPCS_CODE,CLAIM_FROM_MONTH,TOTAL_PAID,TOTAL_CLAIM_LINES,TOTAL_UNIQUE_BENEFICIARIES\n"
                "1234567802,T1019,2024-01-01,100,2,1\n"
                "1234567803,T1019,2024-01-01,80,2,1\n",
                encoding="utf-8",
            )
            from fraudledger.ingest.public import ingest_medicaid

            ingest_medicaid(con, settings, path=str(path))
            con.executemany(
                """
                INSERT INTO providers (
                    provider_key, npi, name, address, city, state, origin
                ) VALUES (?, ?, ?, '10 SHARED WAY', 'PHOENIX', 'AZ', 'public')
                """,
                [
                    ("1234567802", "1234567802", "Linked One"),
                    ("1234567803", "1234567803", "Linked Two"),
                ],
            )
            con.executemany(
                """
                INSERT INTO ownership_edges (
                    owner_key, owner_name, provider_key, percentage, role_code,
                    role_strength, edge_weight, origin
                ) VALUES ('A100', 'Shared Control LLC', ?, 100, '34', 3, 3, 'public')
                """,
                [("1234567802",), ("1234567803",)],
            )
            execute_run(con, settings, run_id="API")
            con.close()
            client = TestClient(create_app(settings))
            health = client.get("/health")
            self.assertEqual(health.status_code, 200)
            self.assertEqual(health.json()["status"], "ok")
            page = client.get("/")
            self.assertEqual(page.status_code, 200)
            self.assertNotIn("199999", page.text)
            self.assertNotIn("demonstration", page.text.lower())
            self.assertNotIn("fictional", page.text.lower())
            groups = client.get("/clusters")
            self.assertEqual(groups.status_code, 200)
            self.assertIn("What this list is highlighting", groups.text)
            self.assertIn("have a billing indicator", groups.text)
            self.assertIn("Shared Control LLC", groups.text)
            self.assertIn("PECOS associate id", groups.text)
            import re
            cluster_ids = list(dict.fromkeys(re.findall(r"/cluster/(\d+)", groups.text)))
            self.assertTrue(cluster_ids)
            detail = client.get(f"/cluster/{cluster_ids[0]}")
            self.assertEqual(detail.status_code, 200)
            self.assertIn("Shared owners", detail.text)
            self.assertIn("Shared addresses", detail.text)
            self.assertIn("PECOS associate id A100", detail.text)
            self.assertEqual(client.post("/demo/remove").status_code, 404)
            self.assertNotIn("fraudster", page.text.lower())
            con = connect(settings.db_path)
            con.execute(
                """
                UPDATE providers
                SET incorporation_date = DATE '2014-05-01'
                WHERE provider_key = '1234567802'
                """
            )
            con.execute(
                "DELETE FROM scores WHERE run_id = 'API' AND provider_key = '1234567802'"
            )
            con.execute(
                """
                INSERT INTO scores (
                    run_id, provider_key, score, tier, flag_count, excess_dollars, rules
                ) VALUES ('API', '1234567802', 3, 'Elevated', 1, 2000, 'R01')
                """
            )
            con.execute(
                """
                INSERT INTO flags (
                    run_id, provider_key, rule_id, metric_value, threshold,
                    peer_value, excess_dollars, detail_json
                ) VALUES (
                    'API', '1234567802', 'R01', 10, 3, 3, 1000,
                    '{"hcpcs":"T1019","rate":10,"median":3,"lines":100,"paid":1000,"peer_scope":"state","state":"AZ"}'
                )
                """
            )
            con.close()
            provider = client.get("/provider/1234567802")
            self.assertEqual(provider.status_code, 200)
            self.assertIn("Incorporation date: 2014-05-01", provider.text)
            self.assertIn("not a Medicare enrollment date", provider.text)
            self.assertIn("Paid per claim line was", provider.text)
            self.assertIn("The score adds a weight", provider.text)
            self.assertIn("$1,000", provider.text)
            self.assertNotIn("$2,000", provider.text)
            self.assertIn("direct owner (CMS role 34)", provider.text)
            self.assertIn("a public file", provider.text)
            self.assertNotIn("peer_scope", provider.text)
            self.assertNotIn("metric ", provider.text.lower())
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
            self.assertNotIn("fictional", page.text.lower())
            self.assertNotIn("demonstration", page.text.lower())
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
            self.assertIn("direct owner (CMS role 34)", page.text)
            self.assertIn("Do not add it to the direct-owner percent", page.text)
            self.assertIn("How to read", page.text)
            self.assertIn("role strength", page.text)
            self.assertIn("post-office box", page.text)
            self.assertIn("middle name", page.text)
            rules = client.get("/rules")
            self.assertEqual(rules.status_code, 200)
            self.assertIn("at most 40", rules.text)
            notes = reading_notes(settings)
            self.assertIn("Direct owner (CMS role 34)", notes)
            self.assertIn("at least 8", notes)
            lowered = (page.text + rules.text + notes).lower()
            for phrase in BANNED:
                self.assertNotIn(phrase, lowered)

    def test_group_sort_key_is_not_a_provider_score(self) -> None:
        import math

        from fraudledger.reference import explain_cluster

        dollars = 25_316_003
        score = 167 * 2 + math.log1p(dollars) + 5
        group = explain_cluster(687, 1168, 167, score, dollars)
        self.assertIn("167 of 687 providers (24%) have a billing indicator", group["headline"])
        self.assertIn("The other 520", group["quiet_line"])
        self.assertIn("The sort key is 356", group["sort_sentence"])
        self.assertIn("334 for the 167 providers", group["sort_sentence"])
        self.assertIn("not a severity score", group["sort_sentence"])
        self.assertNotIn("exclusion list", group["sort_sentence"])
        self.assertFalse(group["has_exclusion"])
        self.assertEqual(group["link"], "Shared owners")


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

    def test_provider_catalog_reads_nested_csv(self) -> None:
        from fraudledger.ingest.catalog import provider_csv_url

        catalog = [
            {
                "title": "Hospice - Provider Data",
                "distribution": [
                    {"data": {"downloadURL": "https://data.cms.gov/hospice.json"}},
                    {"data": {"downloadURL": "https://data.cms.gov/hospice.csv?cache=1"}},
                ],
            },
            {
                "title": "Home Health Care Agencies",
                "distribution": [{"downloadURL": "https://data.cms.gov/home.csv", "mediaType": "text/csv"}],
            },
        ]
        self.assertEqual(
            provider_csv_url(catalog, "Hospice - Provider Data"),
            "https://data.cms.gov/hospice.csv?cache=1",
        )
        self.assertEqual(
            provider_csv_url(catalog, "Home Health Care Agencies"),
            "https://data.cms.gov/home.csv",
        )
        with self.assertRaises(SystemExit):
            provider_csv_url(catalog, "Missing title")

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
            path = Path(tmp) / "spend.csv"
            path.write_text(
                "BILLING_PROVIDER_NPI_NUM,HCPCS_CODE,CLAIM_FROM_MONTH,TOTAL_PAID,TOTAL_CLAIM_LINES,TOTAL_UNIQUE_BENEFICIARIES\n"
                "1234567893,T2042,2024-01-01,100,2,1\n"
                "1234567893,99213,2024-01-01,50,1,1\n",
                encoding="utf-8",
            )
            ingest_medicaid(con, settings, path=str(path), focus="priority")
            codes = {row[0] for row in con.execute("SELECT hcpcs FROM facts_medicaid").fetchall()}
            self.assertEqual(codes, {"T2042"})
            execute_run(con, settings, run_id="PUBLIC")
            dest = build_packet(con, settings, "PUBLIC", "1234567893")
            with zipfile.ZipFile(dest) as archive:
                names = set(archive.namelist())
                narrative = archive.read("narrative.txt").decode("utf-8")
                submit = archive.read("how-to-submit.txt").decode("utf-8")
                pdf = archive.read("report.pdf")
            self.assertEqual(
                names,
                {"narrative.txt", "evidence.csv", "indicators.csv", "how-to-submit.txt", "report.pdf"},
            )
            self.assertIn("What raised your concern", narrative)
            self.assertIn("I read the public Medicaid Provider Spending extract", narrative)
            self.assertIn("1234567893", narrative)
            self.assertNotIn("How to file this", narrative)
            self.assertNotIn("https://america.gov/fraud", narrative)
            self.assertIn("https://america.gov/fraud", submit)
            self.assertIn("Bottom line up front", narrative)
            self.assertIn("What the data indicates", narrative)
            self.assertIn("Dollar figures", narrative)
            self.assertIn("Dollars above a typical peer", narrative)
            self.assertIn("PECOS associate id", narrative)
            self.assertTrue(pdf.startswith(b"%PDF"))
            self.assertGreater(len(pdf), 2000)
            self.assertIn("does not contact", submit.lower())
            blob = (narrative + submit).lower()
            for phrase in BANNED:
                self.assertNotIn(phrase, blob)
            con.close()

    def test_owner_line_names_the_pecos_associate_id(self) -> None:
        from fraudledger.report.packet import _owner_lines

        lines = _owner_lines(
            [("7911382882", "EXTENDED CHHA ACQUISITION, LLC", "34", 100, 3, "public:home-health-owners")]
        )
        self.assertEqual(
            lines,
            ["EXTENDED CHHA ACQUISITION, LLC (PECOS associate id 7911382882): direct owner (CMS role 34) at 100%."],
        )
        blank = _owner_lines(
            [("0941370241", "DAVID HELFGOTT", "40", None, 1, "public:hospice-owners")]
        )
        self.assertIn("CMS stored no percent on this row", blank[0])
        self.assertIn("officer (CMS role 40)", blank[0])
        from fraudledger.report.packet import _explain_flag, _weight_sentence

        text = _explain_flag(
            "R01",
            180.1,
            75.06,
            25.02,
            202844.64,
            '{"hcpcs":"G0156","rate":180.1,"median":25.02,"lines":1308,"paid":235565,'
            '"peer_scope":"national","state":"NY"}',
        )
        self.assertIn("The national median", text)
        self.assertNotIn("national NY", text)
        self.assertNotIn("NY", text)
        self.assertIn("not money taken", text)
        jump = _explain_flag(
            "R07",
            3.0,
            2.0,
            1000,
            3000,
            '{"year":2024,"prior_year":2023,"paid":4000,"prior_paid":1000,"growth":3,'
            '"absolute_change":3000,"patients":20,"prior_patients":10,"patient_growth":1}',
        )
        self.assertIn("2024 was 4.0 times 2023", jump)
        self.assertIn("average monthly patient count", jump)
        weights = _weight_sentence(load_settings(data_dir=self.id()), ["R01", "R02", "R03"], 9.0)
        self.assertIn("R01 is 3", weights)
        self.assertIn("sum to 9.0", weights)
        self.assertNotIn("fraud amount", (text + jump + weights).lower())
        from fraudledger.detect.score import unduplicated_excess

        self.assertEqual(
            unduplicated_excess([("R07", 1000), ("R12", 1000), ("R01", 50)]),
            1050,
        )
        self.assertEqual(unduplicated_excess([("R12", 400)]), 400)
        shared = _explain_flag(
            "R17",
            4,
            3,
            None,
            None,
            '{"servicing_npi":"1891803037","billers":4,"paid":7761185,'
            '"other_billers":[{"npi":"1234567893","name":"Other Biller LLC","paid":1200}]}',
        )
        self.assertIn("Other Biller LLC", shared)
        self.assertIn("NPI 1234567893", shared)
        self.assertIn("paid $1,200", shared)

    def test_packet_names_the_link_and_the_address(self) -> None:
        import zipfile

        from fraudledger.db import refresh_facts_view
        from fraudledger.graph.links import explain_membership
        from fraudledger.report.packet import build_packet

        with tempfile.TemporaryDirectory() as tmp:
            settings = load_settings(data_dir=tmp)
            settings.ensure_dirs()
            con = connect(settings.db_path)
            refresh_facts_view(con, settings.data_dir / "parquet" / "facts")
            con.execute(
                """
                INSERT INTO runs (run_id, started_at, status, demo_only)
                VALUES ('LINK', current_timestamp, 'succeeded', false)
                """
            )
            con.executemany(
                """
                INSERT INTO providers (
                    provider_key, npi, name, entity_type, address, city, state, zip, origin
                ) VALUES (?, ?, ?, 'organization', ?, ?, ?, '11234', 'public')
                """,
                [
                    ("1770513145", "1770513145", "AMERICARE CERTIFIED SPECIAL SERVICES, INC.", "5923 STRICKLAND AVE", "BROOKLYN", "NY"),
                    ("1888888881", "1888888881", "Other Agency", "10 OTHER ST", "BROOKLYN", "NY"),
                    ("1990000001", "1990000001", "Alpha House", "1 SHARED WAY", "AUSTIN", "TX"),
                    ("1990000002", "1990000002", "Beta House", "1 SHARED WAY", "AUSTIN", "TX"),
                    ("1990000003", "1990000003", "Gamma House", "1 SHARED WAY", "AUSTIN", "TX"),
                ],
            )
            con.executemany(
                """
                INSERT INTO ownership_edges (
                    owner_key, owner_name, provider_key, percentage, role_code,
                    role_strength, edge_weight, origin
                ) VALUES (?, 'DAVID HELFGOTT', ?, NULL, '40', 1, 1, 'public:hospice-owners')
                """,
                [("0941370241", "1770513145"), ("0941370241", "1888888881")],
            )
            con.executemany(
                """
                INSERT INTO cluster_members (run_id, cluster_id, provider_key)
                VALUES ('LINK', ?, ?)
                """,
                [(593, "1770513145"), (593, "1888888881"), (7, "1990000001"), (7, "1990000002"), (7, "1990000003")],
            )
            con.execute(
                """
                INSERT INTO clusters (
                    run_id, cluster_id, provider_count, owner_count, flagged_count, score, excess_dollars
                ) VALUES ('LINK', 593, 2, 1, 0, 1, 0), ('LINK', 7, 3, 0, 0, 1, 0)
                """
            )
            con.execute(
                """
                INSERT INTO scores (
                    run_id, provider_key, score, tier, flag_count, excess_dollars, rules
                ) VALUES ('LINK', '1770513145', 1, 'Monitor', 0, 0, '')
                """
            )
            dest = build_packet(con, settings, "LINK", "1770513145")
            with zipfile.ZipFile(dest) as archive:
                narrative = archive.read("narrative.txt").decode("utf-8")
            self.assertIn("Directory address: 5923 STRICKLAND AVE, BROOKLYN, NY", narrative)
            self.assertIn("No other provider in this group uses that street address", narrative)
            self.assertIn("not why this provider is in the group", narrative)
            self.assertIn("DAVID HELFGOTT (PECOS associate id 0941370241)", narrative)
            self.assertIn("officer (CMS role 40)", narrative)
            self.assertIn("Other Agency (NPI 1888888881)", narrative)
            self.assertIn("CMS stored no percent", narrative)
            self.assertNotIn("share an owner or a street address", narrative)
            address_lines = explain_membership(
                con,
                "LINK",
                "1990000001",
                "1 SHARED WAY",
                "AUSTIN",
                "TX",
                [],
                settings.address_link_max,
            )
            joined = " ".join(address_lines)
            self.assertIn("Beta House (NPI 1990000002)", joined)
            self.assertIn("Gamma House (NPI 1990000003)", joined)
            self.assertIn("This address meets that rule, so it is a link", joined)
            self.assertIn("A shared owner is not the direct link", joined)
            con.close()

    def test_group_packet_covers_every_provider(self) -> None:
        import zipfile

        from fraudledger.db import refresh_facts_view
        from fraudledger.report.group import build_group_packet

        with tempfile.TemporaryDirectory() as tmp:
            settings = load_settings(data_dir=tmp)
            settings.ensure_dirs()
            con = connect(settings.db_path)
            refresh_facts_view(con, settings.data_dir / "parquet" / "facts")
            con.execute(
                """
                INSERT INTO runs (run_id, started_at, status, demo_only)
                VALUES ('LINK', current_timestamp, 'succeeded', false)
                """
            )
            con.executemany(
                """
                INSERT INTO providers (
                    provider_key, npi, name, entity_type, address, city, state, zip, origin
                ) VALUES (?, ?, ?, 'organization', ?, ?, ?, '11234', 'public')
                """,
                [
                    ("1770513145", "1770513145", "AMERICARE CERTIFIED SPECIAL SERVICES, INC.", "5923 STRICKLAND AVE", "BROOKLYN", "NY"),
                    ("1888888881", "1888888881", "Other Agency", "10 OTHER ST", "BROOKLYN", "NY"),
                ],
            )
            con.executemany(
                """
                INSERT INTO ownership_edges (
                    owner_key, owner_name, provider_key, percentage, role_code,
                    role_strength, edge_weight, origin
                ) VALUES ('0941370241', 'DAVID HELFGOTT', ?, NULL, '40', 1, 1, 'public:hospice-owners')
                """,
                [("1770513145",), ("1888888881",)],
            )
            con.executemany(
                "INSERT INTO cluster_members (run_id, cluster_id, provider_key) VALUES ('LINK', 593, ?)",
                [("1770513145",), ("1888888881",)],
            )
            con.execute(
                """
                INSERT INTO clusters (
                    run_id, cluster_id, provider_count, owner_count, flagged_count, score, excess_dollars
                ) VALUES ('LINK', 593, 2, 1, 1, 1, 0)
                """
            )
            con.execute(
                """
                INSERT INTO scores (
                    run_id, provider_key, score, tier, flag_count, excess_dollars, rules
                ) VALUES ('LINK', '1770513145', 3, 'Elevated', 1, 0, 'R01')
                """
            )
            con.execute(
                """
                INSERT INTO flags (
                    run_id, provider_key, rule_id, metric_value, threshold, peer_value, excess_dollars, detail_json
                ) VALUES (
                    'LINK', '1770513145', 'R01', 20, 10, 5, 100,
                    '{"hcpcs":"T1019","family_label":"personal care"}'
                )
                """
            )
            dest = build_group_packet(con, settings, "LINK", 593)
            with zipfile.ZipFile(dest) as archive:
                names = set(archive.namelist())
                narrative = archive.read("narrative.txt").decode("utf-8")
                members_csv = archive.read("members.csv").decode("utf-8")
                owners_csv = archive.read("owners.csv").decode("utf-8")
                pdf = archive.read("report.pdf")
            self.assertEqual(
                names,
                {"narrative.txt", "members.csv", "owners.csv", "indicators.csv", "report.pdf"},
            )
            self.assertIn("What raised your concern", narrative)
            self.assertIn("I read the public Medicaid Provider Spending extract", narrative)
            self.assertIn("Ownership group 593", narrative)
            self.assertIn("This one file covers every provider in the group", narrative)
            self.assertIn("AMERICARE CERTIFIED SPECIAL SERVICES, INC.", narrative)
            self.assertIn("Other Agency", narrative)
            self.assertIn("DAVID HELFGOTT (PECOS associate id 0941370241) is on 2 providers", narrative)
            self.assertIn("officer (CMS role 40)", narrative)
            self.assertIn("No street address is used by two providers", narrative)
            self.assertIn("also on Other Agency (NPI 1888888881)", narrative)
            self.assertIn("Providers with no billing indicator", narrative)
            self.assertNotIn("How to file this", narrative)
            self.assertIn("1770513145", members_csv)
            self.assertIn("1888888881", members_csv)
            self.assertIn("0941370241", owners_csv)
            self.assertTrue(pdf.startswith(b"%PDF"))
            lowered = narrative.lower()
            from fraudledger.report.assemble import BANNED
            for phrase in BANNED:
                self.assertNotIn(phrase, lowered)
            con.close()

    def test_purge_keeps_a_public_row(self) -> None:
        from fraudledger.ingest.cleanup import purge_reserved
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
            message = purge_reserved(con, settings)
            self.assertIn("Public rows were kept", message)
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


class AccuracyTests(unittest.TestCase):
    def _ready(self) -> tuple:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        settings = load_settings(data_dir=tmp.name)
        settings.ensure_dirs()
        con = connect(settings.db_path)
        self.addCleanup(con.close)
        return tmp, settings, con

    def _score(self, con, settings, path: Path) -> str:
        ingest_medicaid(con, settings, path=str(path), focus="priority")
        return "ACCURACY"

    def test_expected_family_and_missing_dates(self) -> None:
        from fraudledger.detect.engine import apply_rules

        _tmp, settings, con = self._ready()
        path = settings.data_dir / "facts.csv"
        path.write_text(
            "BILLING_PROVIDER_NPI_NUM,HCPCS_CODE,CLAIM_FROM_MONTH,TOTAL_PAID,TOTAL_CLAIM_LINES\n"
            "7100000001,T1019,2024-06,2000000,10\n"
            "7100000002,T1019,2024-06,1000000,10\n"
            "7100000002,T2042,2024-06,1000000,10\n"
            "7100000003,T1019,2024-06,1200000,10\n"
            "7100000004,T1019,2024-06,1200000,10\n",
            encoding="utf-8",
        )
        run_id = self._score(con, settings, path)
        con.execute(
            """
            INSERT INTO providers (provider_key, npi, name, enumeration_date, origin)
            VALUES ('7100000004', '7100000004', 'Dated', DATE '2024-03-01', 'public')
            """
        )
        apply_rules(con, settings, run_id)
        personal = flags_for(con, run_id, "7100000001")
        mixed = flags_for(con, run_id, "7100000002")
        undated = flags_for(con, run_id, "7100000003")
        dated = flags_for(con, run_id, "7100000004")
        self.assertNotIn("R05", personal)
        self.assertNotIn("R09", personal)
        self.assertIn("R05", mixed)
        self.assertNotIn("R09", undated)
        self.assertIn("R09", dated)
        self.assertNotIn("R05", dated)

    def test_state_peers_suppress_a_high_rate_state(self) -> None:
        from fraudledger.detect.engine import apply_rules

        _tmp, settings, con = self._ready()
        lines = ["BILLING_PROVIDER_NPI_NUM,HCPCS_CODE,CLAIM_FROM_MONTH,TOTAL_PAID,TOTAL_CLAIM_LINES"]
        providers = []
        for index in range(30):
            npi = f"610000{index:04d}"
            lines.append(f"{npi},T1019,2024-06,2000,10")
            providers.append((npi, "TX"))
        lines.append("6100000031,T1019,2024-06,250000,1000")
        providers.append(("6100000031", "TX"))
        for index in range(90):
            npi = f"620000{index:04d}"
            lines.append(f"{npi},T1019,2024-06,400,10")
            providers.append((npi, "CA"))
        lines.append("6200000091,T1019,2024-06,150000,1000")
        providers.append(("6200000091", "CA"))
        path = settings.data_dir / "peers.csv"
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        run_id = self._score(con, settings, path)
        con.executemany(
            """
            INSERT INTO providers (provider_key, npi, name, state, origin)
            VALUES (?, ?, 'Peer', ?, 'public')
            """,
            [(npi, npi, state) for npi, state in providers],
        )
        apply_rules(con, settings, run_id)
        self.assertNotIn("R01", flags_for(con, run_id, "6100000031"))
        self.assertIn("R01", flags_for(con, run_id, "6200000091"))

    def test_owner_middle_name_and_reinstatement(self) -> None:
        from fraudledger.detect.engine import apply_rules

        _tmp, settings, con = self._ready()
        path = settings.data_dir / "owners.csv"
        path.write_text(
            "BILLING_PROVIDER_NPI_NUM,HCPCS_CODE,CLAIM_FROM_MONTH,TOTAL_PAID,TOTAL_CLAIM_LINES\n"
            "3111111111,T1019,2024-06,1000,1\n"
            "3111111112,T1019,2024-06,1000,1\n"
            "3111111113,T1019,2024-01,1000,1\n"
            "3111111114,T1019,2024-06,1000,1\n",
            encoding="utf-8",
        )
        run_id = self._score(con, settings, path)
        con.execute(
            """
            INSERT INTO ownership_edges (
                owner_key, owner_name, provider_key, percentage, role_code, role_strength,
                edge_weight, origin, owner_first, owner_middle, owner_last, owner_state
            ) VALUES
                ('M1', 'ANN MARIE LEE', '3111111111', 100, '34', 3, 3, 'public', 'ANN', 'MARIE', 'LEE', 'TX'),
                ('M2', 'BOB ALAN DOE', '3111111112', 100, '34', 3, 3, 'public', 'BOB', 'ALAN', 'DOE', 'TX'),
                ('M3', 'CARL JONES', '3111111113', 100, '34', 3, 3, 'public', 'CARL', 'J', 'JONES', 'TX'),
                ('M4', 'DANA SMITH', '3111111114', 100, '34', 3, 3, 'public', 'DANA', NULL, 'SMITH', 'TX')
            """
        )
        con.execute(
            """
            INSERT INTO leie (
                npi, name, excl_type, excl_date, first_name, last_name, state, middle_name, reinstated_on, origin
            ) VALUES
                (NULL, 'ANN PAT LEE', '1128a1', DATE '2020-01-01', 'ANN', 'LEE', 'TX', 'PAT', NULL, 'public'),
                (NULL, 'BOB ALAN DOE', '1128a1', DATE '2020-01-01', 'BOB', 'DOE', 'TX', 'ALAN', DATE '2024-06-01', 'public'),
                (NULL, 'CARL JAMES JONES', '1128a1', DATE '2020-01-01', 'CARL', 'JONES', 'TX', 'JAMES', DATE '2024-06-01', 'public'),
                (NULL, 'DANA X SMITH', '1128a1', DATE '2020-01-01', 'DANA', 'SMITH', 'TX', 'X', NULL, 'public')
            """
        )
        apply_rules(con, settings, run_id)
        self.assertNotIn("R13", flags_for(con, run_id, "3111111111"))
        self.assertNotIn("R13", flags_for(con, run_id, "3111111112"))
        self.assertIn("R13", flags_for(con, run_id, "3111111113"))
        self.assertIn("R13", flags_for(con, run_id, "3111111114"))

    def test_servicing_exclusion_is_not_a_billing_exclusion(self) -> None:
        from fraudledger.detect.engine import apply_rules

        _tmp, settings, con = self._ready()
        path = settings.data_dir / "service.csv"
        path.write_text(
            "BILLING_PROVIDER_NPI_NUM,SERVICING_PROVIDER_NPI_NUM,HCPCS_CODE,CLAIM_FROM_MONTH,TOTAL_PAID,TOTAL_CLAIM_LINES\n"
            "4111111111,4222222222,T1019,2024-06,50000,10\n",
            encoding="utf-8",
        )
        run_id = self._score(con, settings, path)
        con.execute(
            """
            INSERT INTO leie (npi, name, excl_type, excl_date, origin)
            VALUES ('4222222222', 'SERVICER', '1128a1', DATE '2020-01-01', 'public')
            """
        )
        apply_rules(con, settings, run_id)
        found = flags_for(con, run_id, "4111111111")
        self.assertIn("R16", found)
        self.assertNotIn("R11", found)

    def test_care_compare_and_state_disagreeing_ccn(self) -> None:
        from fraudledger.detect.engine import apply_rules
        from fraudledger.ingest.public import ingest_care_compare, ingest_medicare_use

        _tmp, settings, con = self._ready()
        lines = ["BILLING_PROVIDER_NPI_NUM,HCPCS_CODE,CLAIM_FROM_MONTH,TOTAL_PAID,TOTAL_CLAIM_LINES"]
        care = ["CMS Certification Number (CCN),State,Measure Name,Score"]
        medicare = ["YEAR,PRVDR_ID,STATE,TOT_EPSD_STAY_CNT,TOT_SRVC_DAYS"]
        enrollments = []
        for index in range(1, 11):
            npi = f"500000{index:04d}"
            ccn = f"{150000 + index:06d}"
            lines.append(f"{npi},T1019,2024-03,1000,1")
            enrollments.append((f"E{index}", npi, ccn, "TX"))
            if index <= 8:
                care.append(f"{ccn},TX,Early live discharges,10")
                medicare.append(f"2024,{ccn},TX,100,5000")
            elif index == 9:
                care.append(f"{ccn},TX,Early live discharges,40")
                care.append(f"{ccn},TX,Burdensome transitions,90")
                medicare.append(f"2024,{ccn},TX,100,20000")
            else:
                care.append(f"{ccn},CA,Early live discharges,90")
                medicare.append(f"2024,{ccn},CA,100,20000")
        facts = settings.data_dir / "facts.csv"
        facts.write_text("\n".join(lines) + "\n", encoding="utf-8")
        care_path = settings.data_dir / "care.csv"
        care_path.write_text("\n".join(care) + "\n", encoding="utf-8")
        medicare_path = settings.data_dir / "medicare.csv"
        medicare_path.write_text("\n".join(medicare) + "\n", encoding="utf-8")
        run_id = self._score(con, settings, facts)
        con.executemany(
            """
            INSERT INTO enrollments (enrollment_id, npi, ccn, state, origin)
            VALUES (?, ?, ?, ?, 'public')
            """,
            enrollments,
        )
        self.assertEqual(ingest_care_compare(con, settings, path=str(care_path), setting="hospice"), 10)
        stored = {
            row[0]
            for row in con.execute("SELECT DISTINCT measure FROM care_compare").fetchall()
        }
        self.assertEqual(stored, {"Early live discharges"})
        ingest_medicare_use(con, settings, path=str(medicare_path), setting="hospice")
        apply_rules(con, settings, run_id)
        ordinary = flags_for(con, run_id, "5000000001")
        outlier = flags_for(con, run_id, "5000000009")
        mismatch = flags_for(con, run_id, "5000000010")
        self.assertNotIn("R18", ordinary)
        self.assertNotIn("R14", ordinary)
        self.assertIn("R18", outlier)
        self.assertIn("R14", outlier)
        self.assertNotIn("R18", mismatch)
        self.assertNotIn("R14", mismatch)

    def test_shared_clinician_names_the_other_billers(self) -> None:
        import json

        from fraudledger.detect.engine import apply_rules
        from fraudledger.detect.score import write_scores

        _tmp, settings, con = self._ready()
        path = settings.data_dir / "shared.csv"
        path.write_text(
            "BILLING_PROVIDER_NPI_NUM,SERVICING_PROVIDER_NPI_NUM,HCPCS_CODE,CLAIM_FROM_MONTH,TOTAL_PAID,TOTAL_CLAIM_LINES\n"
            "6111111111,6222222222,T1019,2024-06,300000,10\n"
            "6111111112,6222222222,T1019,2024-06,20000,1\n"
            "6111111113,6222222222,T1019,2024-06,20000,1\n"
            "6111111114,6222222222,T1019,2024-06,20000,1\n",
            encoding="utf-8",
        )
        run_id = self._score(con, settings, path)
        con.executemany(
            """
            INSERT INTO providers (provider_key, npi, name, origin)
            VALUES (?, ?, ?, 'public')
            """,
            [
                ("6111111111", "6111111111", "Alpha Billing"),
                ("6111111112", "6111111112", "Beta Billing"),
                ("6111111113", "6111111113", "Gamma Billing"),
                ("6111111114", "6111111114", "Related Billing"),
            ],
        )
        con.executemany(
            """
            INSERT INTO ownership_edges (
                owner_key, owner_name, provider_key, percentage, role_code,
                role_strength, edge_weight, origin
            ) VALUES ('OWN1', 'Shared Owner', ?, 100, '34', 3, 3, 'public')
            """,
            [("6111111111",), ("6111111114",)],
        )
        apply_rules(con, settings, run_id)
        detail = con.execute(
            """
            SELECT detail_json FROM flags
            WHERE run_id = ? AND provider_key = '6111111111' AND rule_id = 'R17'
            """,
            [run_id],
        ).fetchone()
        self.assertIsNotNone(detail)
        payload = json.loads(detail[0])
        named = {item["npi"]: item["name"] for item in payload["other_billers"]}
        self.assertEqual(named, {"6111111112": "Beta Billing", "6111111113": "Gamma Billing"})
        con.execute(
            """
            INSERT INTO flags (
                run_id, provider_key, rule_id, metric_value, threshold,
                peer_value, excess_dollars, detail_json
            ) VALUES
                ('JUMP', '6111111111', 'R07', 3, 2, NULL, 5000, '{}'),
                ('JUMP', '6111111111', 'R12', 3, 2, NULL, 5000, '{}'),
                ('JUMP', '6111111111', 'R01', 4, 3, NULL, 100, '{}')
            """
        )
        write_scores(con, settings, "JUMP")
        stored = con.execute(
            "SELECT excess_dollars FROM scores WHERE run_id = 'JUMP' AND provider_key = '6111111111'"
        ).fetchone()[0]
        self.assertEqual(stored, 5100)


class ChartTests(unittest.TestCase):
    def _chart(self, rows: list[str], states: list[tuple[str, str]]):
        path_rows = [
            "BILLING_PROVIDER_NPI_NUM,HCPCS_CODE,CLAIM_FROM_MONTH,TOTAL_PAID,TOTAL_CLAIM_LINES",
            *rows,
        ]
        settings = load_settings(data_dir=self.tmp.name)
        settings.ensure_dirs()
        path = Path(self.tmp.name) / "paid.csv"
        path.write_text("\n".join(path_rows) + "\n", encoding="utf-8")
        self.con = connect(settings.db_path)
        ingest_medicaid(self.con, settings, path=str(path), focus="priority")
        self.con.executemany(
            """
            INSERT INTO providers (provider_key, npi, name, state, origin)
            VALUES (?, ?, 'Example', ?, 'public')
            """,
            [(npi, npi, state) for npi, state in states],
        )
        return settings

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.con = None

    def tearDown(self) -> None:
        if self.con is not None:
            self.con.close()
        self.tmp.cleanup()

    def test_state_median_sits_under_a_high_month(self) -> None:
        import re

        from fraudledger.report.chart import chart_for

        rows = []
        states = [("1390000001", "TX")]
        for month in ("2024-01", "2024-02", "2024-03", "2024-04", "2024-05", "2024-06"):
            rows.append(f"1390000001,G0156,{month},200000,10")
            for index in range(30):
                npi = f"139000{index + 2:04d}"
                rows.append(f"{npi},G0156,{month},10000,10")
                if month == "2024-01":
                    states.append((npi, "TX"))
        settings = self._chart(rows, states)
        monthly = self.con.execute(
            """
            SELECT month, SUM(paid), string_agg(DISTINCT upper(trim(hcpcs)), ',')
            FROM facts_medicaid
            WHERE billing_npi = '1390000001'
            GROUP BY 1
            ORDER BY 1
            """
        ).fetchall()
        chart = chart_for(self.con, settings, "1390000001", "TX", monthly)
        self.assertIn("Medicaid paid by month", chart["svg"])
        self.assertIn("This provider", chart["svg"])
        self.assertIn("TX median", chart["svg"])
        self.assertIn("$0", chart["svg"])
        self.assertIn("home health", chart["note"])
        self.assertIn("$200k", chart["note"])
        self.assertIn("$10k", chart["note"])
        lines = re.findall(r'points="([^"]+)"', chart["svg"])
        self.assertGreaterEqual(len(lines), 2)

        def last_y(points: str) -> float:
            return float(points.split()[-1].split(",")[1])

        self.assertLess(last_y(lines[-1]), last_y(lines[-2]))

    def test_small_state_uses_the_national_median(self) -> None:
        from fraudledger.report.chart import chart_for

        rows = []
        states = [("1390000001", "TX")]
        for index in range(5):
            npi = f"139000{index + 2:04d}"
            states.append((npi, "TX"))
        for index in range(30):
            npi = f"139100{index:04d}"
            states.append((npi, "CA"))
        for month in ("2024-01", "2024-02", "2024-03"):
            rows.append(f"1390000001,G0156,{month},200000,10")
            for npi, state in states[1:]:
                paid = 50000 if state == "TX" else 8000
                rows.append(f"{npi},G0156,{month},{paid},10")
        settings = self._chart(rows, states)
        monthly = self.con.execute(
            """
            SELECT month, SUM(paid), string_agg(DISTINCT upper(trim(hcpcs)), ',')
            FROM facts_medicaid WHERE billing_npi = '1390000001'
            GROUP BY 1 ORDER BY 1
            """
        ).fetchall()
        chart = chart_for(self.con, settings, "1390000001", "TX", monthly)
        self.assertIn("National median", chart["svg"])
        self.assertNotIn("TX median", chart["svg"])
        self.assertIn("national", chart["note"].lower())
        self.assertNotIn("otherwise", chart["note"].lower())


class DownloadTests(unittest.TestCase):
    def test_forbidden_is_retried_with_another_client_type(self) -> None:
        import email
        import urllib.error
        from unittest.mock import patch

        from fraudledger.ingest.common import download

        calls: list[str] = []

        class Body:
            def __init__(self) -> None:
                self.headers = {"Content-Length": "4"}
                self._sent = False

            def read(self, _size: int = -1) -> bytes:
                if self._sent:
                    return b""
                self._sent = True
                return b"ok\n"

            def __enter__(self):
                return self

            def __exit__(self, *_args) -> bool:
                return False

        def fake_urlopen(request, timeout=60):
            agent = request.get_header("User-agent") or ""
            calls.append(agent)
            if "Chrome" not in agent:
                raise urllib.error.HTTPError(
                    request.full_url, 403, "Forbidden", email.message.Message(), None
                )
            return Body()

        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "file.download"
            with patch("fraudledger.ingest.common.time.sleep"), patch(
                "fraudledger.ingest.common.urllib.request.urlopen", fake_urlopen
            ):
                download("https://example.test/file.csv", dest, label="Example")
            self.assertEqual(dest.read_bytes(), b"ok\n")
        self.assertGreaterEqual(len(calls), 2)
        self.assertIn("Chrome", calls[-1])

    def test_repeated_forbidden_names_the_url(self) -> None:
        import email
        import urllib.error
        from unittest.mock import patch

        from fraudledger.ingest.common import download

        def fake_urlopen(request, timeout=60):
            raise urllib.error.HTTPError(
                request.full_url, 403, "Forbidden", email.message.Message(), None
            )

        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "file.download"
            with patch("fraudledger.ingest.common.time.sleep"), patch(
                "fraudledger.ingest.common.urllib.request.urlopen", fake_urlopen
            ):
                with self.assertRaises(SystemExit) as caught:
                    download("https://example.test/blocked.csv", dest, label="Blocked")
        self.assertIn("HTTP 403", str(caught.exception))
        self.assertIn("https://example.test/blocked.csv", str(caught.exception))
        self.assertFalse(dest.exists())


if __name__ == "__main__":
    unittest.main()
