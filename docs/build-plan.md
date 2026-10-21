# Fraud Ledger — Build Plan

Target host: Ubuntu 24.04 LTS
Purpose: a local analytical application that ingests public healthcare and ownership datasets, flags statistical anomalies (starting with hospice and home health), clusters related providers by ownership, and emits a reproducible report plus an audit package. The package is suitable for human review and, if the analyst chooses, as supporting material for a tip. It does not accuse anyone of a crime and it does not submit tips automatically.

This plan is derived from the research in this conversation: the 20 October 2026 DOJ Criminal Tip Rewards Pilot Program, the public-data sources that can actually support quantification, the anomaly rule set, the report schema, and the DuckDB + NetworkX ownership graph.

---

## 1. What the application is

An analyst workstation, not a public website and not a claims system.

It does four things:

1. **Ingest** published files from HHS, CMS, and OIG, recording URL, retrieval time, byte size, and SHA-256.
2. **Detect** billing, quality, and ownership patterns that diverge from peers, using only public aggregates.
3. **Quantify** each lead (metric vs threshold, percentile, estimated dollars above a peer benchmark).
4. **Package** a run as an immutable bundle: methodology, parameters, input hashes, supporting rows, charts, and a human-readable report.

Every screen and every PDF must say: statistical flags are leads, not findings of fraud.

### In scope for v1

- Medicaid Provider Spending (NPI × HCPCS × month, 2018–2024).
- NPPES provider dimension.
- OIG LEIE cross-match.
- CMS hospice and home-health general info, quality, and PECOS ownership.
- Rule engine covering cost, volume, pattern, growth, and exclusion checks.
- Ownership graph and multi-provider clusters.
- JSON evidence package + PDF report + local review UI.
- Monthly refresh via systemd timer.

### Out of scope for v1

- Beneficiary-level or claim-line PHI. Do not seek it. Public files are already aggregated.
- Automatic submission to america.gov/fraud or fraud.gov.
- Promising a reward. Official guidance is a discretionary $10,000–$30,000 if a tip is original and leads to charges. Presidential remarks about a 1% commission are not the written program rules. The app must not calculate or display an expected payout.
- Day-care subsidy fraud and student-aid fraud as automated detectors. Those programs lack a public billing grain comparable to Medicaid. Leave a rule-pack slot; do not fake a detector.
- Real-time CMS claims. FPS and UPIC models are not public.

### Language contract

Use “anomaly”, “indicator”, “peer deviation”, and “review lead”. Never “fraudster”, “guilty”, “stole”, or “confirmed fraud” in generated text. A lead becomes an allegation only if a human writes one, outside the generator.

---

## 2. Why Ubuntu 24.04, and what hardware it needs

The fact table is about 227 million rows (~3.5 GB compressed, ~10–11 GB CSV, smaller as Parquet). DuckDB can scan Parquet out of core, but peer benchmarks and the ownership graph want memory and fast disk.

| Profile | CPU | RAM | Disk | Fit |
|---|---|---|---|---|
| Minimum dev | 4 cores | 16 GB | 150 GB SSD | Sampled years or one state. Full national scan will spill and be slow. |
| Recommended | 8 cores | 64 GB | 500 GB NVMe | Full national pipeline, comfortable. |
| Analyst workstation | 8–16 cores | 64–128 GB | 1 TB NVMe | Full history plus Open Payments plus several report generations. |

OS packages to install once:

- `python3.12`, `python3.12-venv`, `python3-pip`
- `build-essential`, `pkg-config`
- WeasyPrint native deps if PDF is rendered that way: `libcairo2`, `libpango-1.0-0`, `libpangocairo-1.0-0`, `libgdk-pixbuf-2.0-0`, `libffi-dev`, `shared-mime-info`
- `ca-certificates`, `curl`, `unzip`, `git`
- Optional: `typst` (single static binary) instead of WeasyPrint, which avoids the Cairo stack

Do not install DuckDB from Ubuntu’s apt archive for the analytical engine. Pin the Python wheel (`duckdb`) so file format and SQL dialect match the code. Python 3.12 is the 24.04 default; use a venv or `uv`, not system site-packages.

Suggested layout on the host:

```
/opt/fraudledger/app          # git checkout, venv
/var/lib/fraudledger/raw      # immutable downloads, content-addressed
/var/lib/fraudledger/parquet  # cleaned tables
/var/lib/fraudledger/warehouse/ledger.duckdb
/var/lib/fraudledger/runs     # one directory per pipeline run
/var/log/fraudledger
/etc/fraudledger/config.yaml  # thresholds, paths, source URLs
```

Run the service as a dedicated user `fraudledger` with write access only under `/var/lib/fraudledger` and `/var/log/fraudledger`. The app never needs root at runtime.

---

## 3. Architecture

```
                systemd timer (monthly)
                         |
                         v
  +---------------- ingest workers ----------------+
  | medicaid  nppes  leie  hospice  hha  openpay   |
  | each: download -> sha256 -> raw store -> log   |
  +------------------------+-----------------------+
                           |
                           v
              DuckDB + Parquet lake
              providers, facts, ownership_edges,
              benchmarks, flags, scores, clusters
                           |
            +--------------+--------------+
            |                             |
            v                             v
     rule engine (YAML)          ownership graph
     SQL metrics in DuckDB       NetworkX Louvain
            |                             |
            +-------------+---------------+
                          v
                 scoring + evidence extract
                          |
                          v
            run bundle (json, csv, pdf, hashes)
                          |
                          v
              FastAPI review UI (localhost)
              queue, cluster view, report download
```

Process model:

- **CLI** (`fraudledger`) is the source of truth. The UI calls the same library, it does not reimplement rules.
- **Library** is importable and unit-tested without the UI.
- **DuckDB file** holds dimensions, flags, scores, runs, and audit rows. Monthly facts stay in Parquet and are queried with `read_parquet`, not loaded into SQLite.
- **One run = one directory** that can be copied to another machine and re-hashed.

No Vercel, no serverless, no browser-side crunching of the fact table. A later read-only web UI can sit in front of finished run bundles, but v1 is a local service bound to `127.0.0.1`.

---

## 4. Repository layout

```
fraudledger/
  pyproject.toml
  README.md
  config/
    default.yaml              # thresholds, source URLs, role weights
    role_strength.yaml
  src/fraudledger/
    cli.py                    # typer or argparse: ingest, build, detect, report, serve
    settings.py               # pydantic settings, paths from env/config
    ingest/
      snapshot.py             # content-addressed store + manifest row
      medicaid.py
      nppes.py
      leie.py
      cms_provider.py         # hospice + hha care compare + ownership
      open_payments.py        # optional, phase 3
    warehouse/
      schema.sql
      load.py                 # parquet registration, provider dimension
    detect/
      benchmarks.sql
      rules.yaml              # declarative tests
      engine.py               # interprets rules.yaml -> flag table
      hospice.py              # quality-specific rules
      score.py
    graph/
      edges.sql               # the ownership edge SQL
      cluster.py              # NetworkX Louvain + address edges
    report/
      assemble.py
      templates/report.html.jinja
      pdf.py
      manifest.py             # hashes of every artifact in the run
    api/
      app.py                  # FastAPI
      routes_runs.py
      routes_providers.py
    ui/                       # server-rendered or small Vite build
  tests/
    fixtures/                 # tiny synthetic parquet, not real NPIs of concern
    test_rules.py
    test_edges.py
    test_manifest.py
    test_language.py          # generated text must not contain banned words
  deploy/
    fraudledger.service
    fraudledger-ingest.service
    fraudledger-ingest.timer
  scripts/
    bootstrap-ubuntu.sh       # apt + user + dirs, idempotent
```

---

## 5. Data sources and ingest contract

Each adapter implements the same contract:

```
fetch(url) -> bytes
sha256(bytes) -> hex
store at raw/{sha256[:2]}/{sha256}   # immutable, never overwrite
insert snapshot row: source_id, url, retrieved_at, bytes, sha256, http_etag, note
convert to parquet under parquet/{source_id}/{snapshot_id}/
```

If the hash matches an existing snapshot, skip conversion and record “already present”.

| Source | URL | Grain | v1 priority |
|---|---|---|---|
| Medicaid Provider Spending | https://opendata.hhs.gov/datasets/medicaid-provider-spending/ | billing NPI × servicing NPI × HCPCS × month, 2018–2024 | P0 |
| NPPES monthly V2 | https://download.cms.gov/nppes/NPI_Files.html | one row per NPI | P0 |
| OIG LEIE CSV | https://oig.hhs.gov/exclusions/ | excluded individual/entity | P0 |
| Hospice general + quality | https://data.cms.gov/provider-data/topics/hospice-care | provider / quality measure | P0 |
| Hospice All Owners | https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/hospice-all-owners | owner × enrollment | P0 |
| Home health Care Compare | https://data.cms.gov/provider-data/topics (home health topic) | provider / quality | P0 |
| Home health owners | data.cms.gov, HHA All Owners dataset | owner × enrollment | P0 |
| PAC utilization | https://data.cms.gov/provider-summary-by-type-of-service/medicare-post-acute-care-hospice | provider-year utilization | P1 |
| Open Payments | https://openpaymentsdata.cms.gov/datasets | payment to covered recipient | P2 |
| USAspending | https://api.usaspending.gov/ | awards | later, not medical |
| SAM.gov exclusions | https://sam.gov/ | debarment | P2 |

Column names in CMS ownership files use labels like `ASSOCIATE ID - OWNER` and `TYPE - OWNER`. The loader maps those to stable internal names (`associate_id`, `owner_type`, `percentage`, `role_code`) in one place. When CMS renames a column, only the mapper changes, and the snapshot hash still proves which file was used.

NPPES is a large CSV inside a zip, plus practice-location and other-name reference files. Load the main dissemination file first. Pull additional practice locations only for address clustering.

LEIE downloadable CSV does not include SSN. Match on NPI when present, else do not fuzzy-match common names automatically. A name-only hit is a “possible match” with `match_confidence = low` and is never a hard flag by itself.

---

## 6. Warehouse model

DuckDB database `ledger.duckdb` plus external Parquet.

### Snapshot and audit (small, in DuckDB)

- `snapshots(snapshot_id, source_id, url, retrieved_at, byte_count, sha256, note)`
- `runs(run_id, started_at, finished_at, git_sha, config_sha256, status)`
- `run_inputs(run_id, snapshot_id)` — exactly which files this run read
- `run_artifacts(run_id, path, sha256, kind)` — every file written

### Dimensions

- `providers(provider_key, npi, enrollment_id, ccn, name, entity_type, taxonomy, address, city, state, zip, enumeration_date, deactivated)`
- `provider_key` = NPI if present, else enrollment id / CCN. Never invent a key from a name.

### Facts (Parquet, not a giant DuckDB table)

`facts/medicaid/snapshot_id/*.parquet` columns:

- `billing_npi`, `servicing_npi`, `hcpcs`, `month`
- `beneficiaries`, `claim_lines`, `paid`

Partition by year or by `month` so a single-year rebuild does not rewrite history.

### Derived tables (DuckDB, rebuilt per run)

- `benchmarks(hcpcs, peer_group, n, median_paid_per_line, p90, p99, mean, stddev)`
- `provider_year(npi, year, paid, claim_lines, beneficiaries, distinct_hcpcs, months_active)`
- `flags(run_id, provider_key, rule_id, metric_value, threshold, peer_value, excess_dollars, detail_json)`
- `scores(run_id, provider_key, score, tier, flag_count, excess_dollars)`
- `ownership_edges(...)` as specified below
- `clusters(run_id, cluster_id, provider_count, owner_count, score, excess_dollars)`
- `cluster_members(run_id, cluster_id, provider_key)`

Peer group for v1 is the HCPCS code itself (national). Specialty peer groups are a v1.1 refinement once taxonomy coverage from NPPES is measured. Do not compare a personal-care agency to an emergency department.

---

## 7. Detection rules

Rules live in `config` / `rules.yaml`, not in scattered Python. The engine renders one SQL statement per rule against DuckDB and inserts into `flags`. Changing a threshold changes the config hash stored on the run, so old reports stay interpretable.

### Billing rules (implement in this order)

| ID | Rule | Threshold (starting point) | Excess definition |
|---|---|---|---|
| R01 | Cost outlier, code-specific | paid per claim line > 3× national median for that HCPCS, and paid > $100k on that code | `(rate - median) * lines` |
| R02 | Multi-code rate outlier | above p90 on 2+ HCPCS codes, each with material dollars | sum of per-code excess |
| R03 | High claims per beneficiary | > peer p99 or absolute > 100 lines/beneficiary, min beneficiaries | — |
| R04 | Velocity | claim lines per active month imply ≥ 50 lines per working day | — |
| R05 | Narrow mill | 1–2 HCPCS codes and paid > $1M | — |
| R06 | Flat billing | CV of monthly paid < 0.10 across ≥ 6 active months and paid > $250k | — |
| R07 | Billing swing | YoY > 200% and absolute change > $1M | delta above prior year |
| R08 | Explosive growth | YoY > 500% and paid > $250k | — |
| R09 | New entrant | first NPPES year or first fact year in window, paid > $1M in first 12 months | — |
| R10 | Very new high volume | enumerated 2022 or later and lifetime paid > $5M | — |
| R11 | LEIE hard match | NPI on current LEIE and paid > 0 after exclusion date if date is known | all paid after exclusion |
| R12 | LEIE name-only | name match without NPI | never auto-tiered Critical |

Benford and CUSUM are phase 2. They are easy to misread on rounded payment amounts and should not gate v1.

### Hospice / home health rules (phase 2, after the join works)

- H01 high live-discharge rate vs national hospice peer, combined with high paid.
- H02 long length of stay vs peer, combined with weak quality scores.
- H03 address cluster: ≥ 3 agencies sharing a normalized street address, combined paid above a floor.
- H04 ownership cluster that also contains ≥ 1 provider with any R-rule or H-rule.

Quality fields differ between file vintages. Map measure IDs in a dictionary and skip a rule if the measure is absent from that snapshot, recording the skip in the run log. Do not impute.

### Scoring

```
score = sum(rule.weight for each distinct rule)
      + 2.0 if flag_count >= 3
      + 3.0 if R11
      + 1.5 if member of a multi-provider ownership cluster that contains another flagged provider
```

Tiers:

- Critical: score ≥ 8 or R11
- High: score ≥ 5
- Elevated: score ≥ 3
- Monitor: 1–2 weak flags

Weights live in YAML next to thresholds. R01 and R11 default higher than R06.

### Quantification rules

- Excess dollars are an **estimate versus a peer median**, labeled as such. They are not “fraud amount” and not “recoverable amount”.
- Show the formula in the finding: `(provider_rate - national_median) × claim_lines`, with both rates printed.
- If median is based on fewer than 30 providers for that HCPCS, suppress R01 and record `insufficient_peer_n`.
- Round money to the dollar. Keep full precision in `detail_json`.

---

## 8. Ownership graph

### Edge construction (DuckDB)

Implement the SQL already sketched, with these fixes baked in:

- Normalize NPI with `lpad`.
- `provider_key = coalesce(npi, enrollment_id)`.
- Keep an edge when percentage ≥ 5 **or** role is officer, director, managing employee, or operational control.
- Role strength table (starting values):

| Code | Meaning | Strength |
|---|---|---|
| 01, 34 | 5%+ direct ownership | 3.0 |
| 35 | 5%+ indirect | 2.5 |
| 40, 41 | officer, director | 3.0 |
| 38, 43 | general partner, operational control | 2.5 |
| 03, 25, 42 | partner, managing employee | 2.0 |
| other | — | 1.0 |

- `edge_weight = role_strength * coalesce(percentage, 5) / 100`.
- `edge_source_id` = enrollment + associate id + role code, stored on the edge.
- Do not fuzzy-merge owner names in v1. Associate ID is the only owner key. Unmatched owners stay as their own nodes.

Address edges are separate and weaker (`weight = 1.2`), built from a hash of normalized street + city + state. They connect providers, not owners. Add them only after ownership communities exist, and tag `edge_type = 'address'` so a report can say which link was used.

### Community detection (NetworkX)

- Build an undirected weighted graph from `ownership_edges`.
- Run Louvain (`networkx.algorithms.community.louvain_communities`) with `weight="weight"`, `resolution=1.0`, `seed=42`.
- Discard communities with fewer than 2 providers.
- Persist `cluster_members` before any scoring so billing joins are SQL, not Python loops over 227 M rows.
- If a community exceeds a few hundred providers, re-run Louvain on that subgraph at `resolution=1.5` and keep the finer split, noting both ids (`parent_cluster_id`). A giant “same large health system” component is not a useful lead by itself.

Cluster score:

```
flagged_count * 2
+ ln(1 + cluster_excess)
+ 0.5 * provider_count   # capped so size alone cannot dominate
+ 3 if any member has R11
```

Rank clusters for the review queue ahead of isolated single-flag providers.

---

## 9. Report and audit bundle

One directory per run:

```
runs/2026-10-21T120000Z/
  MANIFEST.json           # sha256 of every file below, plus input snapshot ids
  config.snapshot.yaml    # exact thresholds used
  methodology.md          # frozen prose, generated from the rule catalog
  summary.json
  providers.jsonl         # one object per tier >= Elevated
  clusters.jsonl
  evidence/{provider_key}.csv    # supporting monthly or HCPCS rows, capped
  charts/{provider_key}_monthly.svg
  report.pdf
  report.html
```

`MANIFEST.json` fields: `run_id`, `started_at`, `finished_at`, `git_sha`, `config_sha256`, `inputs[]` (source, url, sha256), `artifacts[]` (relative path, sha256, bytes). The last step hashes the manifest’s artifact list and stores `manifest_sha256` in DuckDB. Regenerating the PDF without rerunning detection must fail the manifest check if inputs changed.

### Report sections

1. Cover — “Statistical review leads”, date, run id, short disclaimer.
2. Executive counts — providers scored, counts by tier, sum of excess estimates, top 15 clusters.
3. Methodology — data sources with retrieval dates and hashes, peer definition, every rule id with its threshold, what was skipped.
4. Findings — for each Critical/High provider: identity, score, each flag with metric and threshold, excess formula, ownership cluster id, LEIE status.
5. Cluster appendix — owners, member providers, combined paid, which edges (ownership vs address).
6. Limits — aggregates only, no medical-record review, no beneficiary identity, legitimate outliers exist, not a legal determination.

PDF via Typst or WeasyPrint from one HTML/Jinja template so HTML and PDF cannot drift. Charts are SVG generated in Python (matplotlib or plain SVG). No screenshots of a browser as evidence.

### Tip package (export, not submit)

A button or CLI flag `fraudledger export-tip --run --provider` writes a zip containing the finding, the methodology section, the evidence CSV, and the manifest. The analyst pastes narrative into the america.gov/fraud intake form themselves. The app stores the export time locally. It does not POST anywhere.

---

## 10. Review UI

Local FastAPI app, bound to `127.0.0.1:8088` (not exposed). Server-rendered pages are enough; a SPA is unnecessary for v1.

Screens:

1. **Runs** — list, status, input hashes, link to PDF.
2. **Queue** — filter by tier, state, rule id, cluster. Default sort: cluster score, then provider score.
3. **Provider** — identity, sparkline of monthly paid, flag table, evidence preview, cluster mates, disclaimer.
4. **Cluster** — owners, providers, shared address flag, combined excess.
5. **Rule catalog** — read-only view of the YAML that produced this run, so an analyst sees the threshold without opening the server.

No accounts in v1. The host is single-analyst. If more than one person will use it, put it behind SSH or a reverse proxy with Ubuntu auth; do not build a second login system until then.

Do not put a “mark as fraud” button. Allowed analyst states: `unreviewed`, `watch`, `dismissed`, `exported`. Dismissal stores a note and never deletes the flag row.

---

## 11. Build phases

Each phase ends with a command that runs on a laptop-sized fixture and on a one-state or one-year slice before the national file is attempted.

### Phase 0 — Host and skeleton (1–2 days)

- `bootstrap-ubuntu.sh` creates user, directories, venv, and package pins.
- `fraudledger --version` and `fraudledger doctor` (Python, DuckDB, disk free, config loads).
- Empty DuckDB schema applied.
- Unit test harness with a 50-provider synthetic parquet (hand-built numbers, fictional NPIs in a reserved test range such as `1999990000+`).

Exit: `pytest` green, no network required.

### Phase 1 — Ingest and provenance (3–5 days)

- Adapters for Medicaid spending, NPPES V2, LEIE.
- Content-addressed raw store and `snapshots` table.
- CSV/ZIP → Parquet with an explicit column map and a row-count assertion.
- `fraudledger ingest medicaid` is resumable and idempotent.

Exit: second ingest of the same file does not duplicate bytes; `snapshots.sha256` matches `sha256sum` on disk.

### Phase 2 — Benchmarks and five rules (4–6 days)

- R01, R04, R06, R09, R11 only.
- National HCPCS median with `n >= 30` guard.
- `flags` and `scores` written for one calendar year.
- JSONL findings for tier ≥ Elevated.

Exit: synthetic fixture has one known cost outlier, one flat biller, one excluded NPI. Tests assert those three flags and assert the clean providers are not flagged.

### Phase 3 — Remaining billing rules and excess dollars (3–4 days)

- R02, R03, R05, R07, R08, R10.
- Excess formula locked and printed in `detail_json`.
- Score tiers.

Exit: a golden JSON file for the fixture; any threshold change fails the test until the golden file is intentionally updated.

### Phase 4 — Hospice and home health join (4–5 days)

- Ingest Care Compare + All Owners for both provider types.
- Provider dimension gains CCN and ownership.
- H03 address cluster.
- H01/H02 only if the quality columns are present in the downloaded dictionary; otherwise the run log says skipped.

Exit: a fixture of three agencies at one address under one associate id produces one cluster and no false cluster across different cities.

### Phase 5 — Graph (3–4 days)

- `ownership_edges` SQL as specified.
- Louvain with fixed seed.
- Membership table joined to scores.
- Cluster ranking.

Exit: fixture graph of two owners and four providers yields the expected communities. Seed change is the only way communities differ between runs on identical edges.

### Phase 6 — Report bundle (3–4 days)

- HTML + PDF from one template.
- MANIFEST with hashes.
- Banned-word test on the rendered HTML.
- `fraudledger verify-run <dir>` recomputes hashes and exits non-zero on tamper.

Exit: flip one byte of an evidence CSV and verify fails.

### Phase 7 — Review UI and scheduler (4–5 days)

- FastAPI pages listed above.
- systemd service for the UI, systemd timer for monthly ingest + detect.
- `fraudledger report` runnable from cron without a display.

Exit: timer unit is `Persistent=true` so a powered-off month catches up; UI shows the latest successful run only after the manifest verifies.

### Phase 8 — Hardening (ongoing)

- Open Payments join as a contextual column, not a rule that fires alone (a payment is not an anomaly).
- PAC utilization join for Medicare hospice/HHA dollars beside Medicaid dollars, labeled by program so they are not added blindly.
- Differential report: providers that newly crossed a tier since the previous run.
- Backup story: nightly `restic` or `duplicity` of `/var/lib/fraudledger` excluding nothing (raw files are the audit trail).

National Medicaid ingest is expected to take on the order of tens of minutes to a few hours on the recommended box, dominated by download and CSV parse the first time. Later runs only convert a new snapshot. Do not optimize before that first full ingest is measured; record timings in the run row.

---

## 12. Configuration

`/etc/fraudledger/config.yaml` (mode 0640, group `fraudledger`):

```yaml
data_dir: /var/lib/fraudledger
bind: 127.0.0.1:8088
min_peer_n: 30
min_ownership_pct: 5
louvain_resolution: 1.0
louvain_seed: 42
rules:
  R01: { multiple_of_median: 3.0, min_paid: 100000, weight: 3 }
  R04: { lines_per_working_day: 50, weight: 2 }
  R06: { max_cv: 0.10, min_months: 6, min_paid: 250000, weight: 2 }
  R09: { min_first_year_paid: 1000000, weight: 2 }
  R11: { weight: 5 }
```

Environment variables only for paths and log level (`FRAUDLEDGER_CONFIG`, `FRAUDLEDGER_DATA_DIR`). No API keys are required for the public file downloads. If a source starts requiring a token, put it in a root-owned file mode 0640 and reference the path, not the secret, in YAML.

---

## 13. Testing strategy

- **Fixture tests** for every rule: one positive, one negative, one “just below threshold”.
- **Schema contract tests**: column renames in a fake CMS header fail the mapper with a clear error, not a silent empty column.
- **Manifest tests**: verify, tamper, verify-fail.
- **Language tests**: render the fixture report and reject banned strings.
- **Graph tests**: small hand-built edge list, exact community membership.
- **No network in CI**. Adapters are tested against local zip/csv fixtures. A separate manual `ingest` against the real URL is an operator step, not a unit test.
- **Numeric stability**: medians computed in DuckDB, asserted to the cent on the fixture.

Do not commit real LEIE names or real high-billing NPIs into git. Fixtures are synthetic.

---

## 14. Operations on Ubuntu 24.04

`fraudledger.service`:

- `User=fraudledger`
- `ExecStart=/opt/fraudledger/app/.venv/bin/fraudledger serve`
- `Restart=on-failure`
- `NoNewPrivileges=true`
- `ProtectSystem=strict`
- `ReadWritePaths=/var/lib/fraudledger /var/log/fraudledger`

`fraudledger-ingest.timer`:

- monthly, `OnCalendar=*-*-03 02:00` (CMS ownership is often quarterly; a monthly timer that no-ops when the hash is unchanged is simpler than a calendar of CMS releases)
- `Persistent=true`

Logging: stdout to journald. One JSON line per ingest and per rule (`event`, `run_id`, `rule_id`, `flagged`, `seconds`).

Disk alerts: if free space under `data_dir` is below 30 GB, `doctor` and the timer refuse to start a new download.

Updates: pull git, `uv sync` or `pip install -e .`, restart the service. DuckDB files are forward-compatible within a pinned major version; pin `duckdb` in `pyproject.toml` and upgrade it deliberately, with a backup of `ledger.duckdb` first.

---

## 15. Security and legal boundaries

- Public data only. If a download URL 404s or starts asking for a CMS data-use agreement, stop. Do not scrape portals that require an account.
- The Medicaid file has no beneficiary identifiers. Do not join outside datasets in an attempt to re-identify people.
- Generated text is a lead list for a human. The human decides whether anything is a tip.
- Retain raw snapshots. Deleting them destroys the audit trail.
- Bind the UI to localhost. If it must be reached remotely, use SSH tunneling, not `0.0.0.0` on a public interface.
- Log analyst exports (who via OS user, when, which provider). Do not log anything that is not already in the public files.

---

## 16. Risks and how the plan handles them

| Risk | Handling |
|---|---|
| Column layout changes on CMS/HHS refresh | Explicit mapper + contract test; ingest fails closed |
| 2024 Medicaid rows marked preliminary | Stamp `provisional=true` on that year in the report |
| National median dominated by a few mega-providers | Also store a winsorized or p50 computed after dropping p99 providers; show both if they diverge a lot |
| Health-system ownership components with hundreds of NPIs | Second-pass Louvain at higher resolution; do not alert on size alone |
| LEIE false name matches | NPI match only for hard flags |
| Mistaking excess-over-median for loss | Label and formula required in every finding; banned word “fraud amount” |
| First full CSV parse blows memory | Convert with DuckDB `read_csv` streaming into partitioned Parquet; do not load via pandas |
| Program rules differ from press statements | App cites the DOJ page’s $10k–$30k discretionary award and does not compute commissions |

---

## 17. Definition of done for v1

An operator on a fresh Ubuntu 24.04 box can:

1. Run the bootstrap script and `fraudledger doctor`.
2. Ingest Medicaid, NPPES, LEIE, hospice ownership, and home-health ownership.
3. Run detection with the committed default config.
4. Open the local queue and see Critical/High leads with a stated metric, threshold, peer value, and excess formula.
5. Open a cluster that shares an associate id and see its members together.
6. Download a PDF whose manifest verifies.
7. Tamper with one evidence file and watch `verify-run` fail.
8. Re-run ingest on unchanged files and see no duplicate data and the same scores.

Until those eight are true, do not add Open Payments, USAspending, Benford, or a public-facing site.

---

## 18. Suggested first coding tasks (in order)

1. `pyproject.toml`, package skeleton, `doctor`, DuckDB schema migration.
2. Synthetic fixture generator and the five Phase 2 tests written **before** the SQL, so the rules are pinned by examples.
3. Medicaid + NPPES + LEIE ingest into Parquet.
4. Benchmark SQL and R01/R11.
5. Flag insert + JSONL dump.
6. Ownership edge SQL + fixture graph test.
7. Louvain membership.
8. HTML report + manifest verify.
9. FastAPI queue on top of the latest verified run.
10. systemd units.

Do not start with the UI. The audit bundle is the product; the UI is a viewer for it.
