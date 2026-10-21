# Fraud Ledger

Local review workstation for public healthcare billing anomalies. It loads public extracts, scores peer deviations, and writes a hashed evidence bundle. It does not decide that anyone committed fraud, and it does not submit a tip.

Statistical flags are review leads, not findings of fraud. Excess dollars are a gap versus a peer median, not a loss amount and not a recoverable amount. A person decides whether any lead is worth sending to [america.gov/fraud](https://america.gov/fraud). The Department of Justice Criminal Tip Rewards Pilot is discretionary. This program does not compute a reward.

The review UI binds to `127.0.0.1:8088`. It is an analyst tool for one desktop, not a public website.

## Install

One Ubuntu Desktop machine, 24.04 LTS or 26.04 LTS:

```bash
git clone https://github.com/jhenrysec/fraudledger.git
cd fraudledger
bash install.sh
```

That uses sudo once. It installs Python packages into `/opt/fraudledger/venv`, creates a `fraudledger` system user, writes `/etc/fraudledger/config.yaml` if it is missing, loads a fictional demonstration set, and starts a systemd service. Then open `http://127.0.0.1:8088/` in the desktop browser. Wayland versus X11 does not matter; this is a local web page.

Ubuntu 26.04 Desktop's default Python is 3.14. The installer uses that interpreter and installs a DuckDB build that already has a 3.14 wheel, so it does not compile a database engine on the machine.

```bash
bash install.sh --user       # virtualenv in this clone, no systemd
bash install.sh --no-demo    # do not load fictional NPIs
bash install.sh --no-start   # install services but do not start the UI
```

The demonstration set, the LEIE, and the hospice or home-health owner files are comfortable on one desktop. The national Medicaid extract is multi-gigabyte. Give that pull a large free disk (hundreds of GB if you keep the raw file and the Parquet) and at least 16 GB of RAM. A smaller desktop can still run the demonstration and the smaller public files.

The `--user` command wires `.venv/bin/fraudledger` at this clone's `var/config.yaml`. The system command uses `/etc/fraudledger/config.yaml` and does not depend on the clone staying on disk.

Re-running the installer does not overwrite an existing config file. Demonstration rows use fictional NPIs in the `199999xxxx` range. `fraudledger ingest medicaid --drop-demo` removes them when a real extract is loaded.

## What a run stores

Each run directory under the data directory (`/var/lib/fraudledger/runs/<id>` by default) contains:

- `report.txt`, `report.html`, `report.pdf`
- `config.snapshot.yaml` and `summary.json`
- `evidence/<provider>.csv`
- `MANIFEST.json` with SHA-256 for every other file in the run

`fraudledger verify-run <id>` exits non-zero if a file is missing or was edited. Dismissal in the UI changes a review state. It does not delete flag rows.

## Commands

```bash
fraudledger doctor
fraudledger demo
fraudledger ingest medicaid --path spending.csv
fraudledger ingest medicaid --url "$MEDICAID_URL" --drop-demo
fraudledger ingest leie --path UPDATED.csv
fraudledger ingest nppes --path NPPES_Data_Dissemination.zip
fraudledger ingest ownership --path hospice_owners.csv --kind hospice_owners
fraudledger run
fraudledger verify-run 20261021T120000Z
fraudledger export-tip 20261021T120000Z
fraudledger review 1999990001 --state watch --note "Compare to the enrollment file"
fraudledger serve
fraudledger sources
fraudledger pull leie
fraudledger pull hospice
fraudledger pull medicaid --drop-demo
fraudledger pull --url "$FILE_URL" --as medicaid --drop-demo
fraudledger pull --all
fraudledger refresh
```

`fraudledger sources` lists the known public files. `pull` looks up the current download for that file. `pull --url` uses an address you paste, and `--as` says how to read it (`medicaid`, `leie`, `nppes`, `hospice-owners`, `hospice-enrollments`, `home-health-owners`, `home-health-enrollments`). The same two choices are on the Sources page at `http://127.0.0.1:8088/sources`.

Hospice and home health pulls also load the matching enrollment file. The owner extract is keyed by enrollment id; the enrollment extract supplies the NPI, which is what the billing rows use.

`refresh` still downloads only the URLs saved in the config. If every URL is empty, it exits 0 and downloads nothing. Put a URL in `sources:` when you want the monthly timer to fetch that file instead of looking it up. The timer runs on the 3rd day of the month at 02:00.

`pull --all` fetches every known source, including the multi-gigabyte Medicaid extract. It is not what the timer runs.

`export-tip` only zips the evidence package. It does not contact the government.

## Public sources

Set these in `/etc/fraudledger/config.yaml` only when you want `refresh` to use a fixed URL. Leave them empty to let `fraudledger pull` find the current file.

| Source | Where the pull looks |
| --- | --- |
| Medicaid Provider Spending | https://opendata.hhs.gov/datasets/medicaid-provider-spending/ |
| NPPES monthly file | https://download.cms.gov/nppes/NPI_Files.html |
| OIG LEIE | https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv |
| Hospice owners and enrollments | https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/hospice-all-owners |
| Home health owners and enrollments | https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/home-health-agency-all-owners |

v1 scores hospice and home-health patterns that show up in the Medicaid spending extract, plus an NPI match to the LEIE. Owner linking uses the CMS associate id only. A shared address is an extra edge when at least three providers use the same street, city, and state. Name-only LEIE matches are not flags.

The national Medicaid extract is on the order of 227 million rows. Do not open it in pandas. The loader streams CSV into partitioned Parquet through DuckDB. Plan on a large disk (the build notes use 500 GB) before pointing `medicaid_url` at the full file. A laptop can run the demonstration set.

## Rules

Thresholds live in the YAML config and the hash of that file is stored on the run.

| Rule | Lead |
| --- | --- |
| R01 | Paid per line above 3× the national HCPCS median, at least $100k, peer group of 30 |
| R02 | Above the 90th percentile rate on two or more codes |
| R03 | Lines divided by the busiest month's beneficiary count, over 100 |
| R04 | Lines per working day at least 50 |
| R05 | One or two codes and at least $1M paid |
| R06 | Coefficient of variation of monthly paid under 0.10, six months, $250k |
| R07 | Year-over-year growth over 2 and an absolute change of at least $1M |
| R08 | Year-over-year growth over 5 and latest-year paid at least $250k |
| R09 | First-year paid at least $1M and enumeration is not older than that year |
| R10 | Enumerated on or after 2022-01-01 and lifetime paid at least $5M |
| R11 | NPI is on the LEIE and paid amounts fall on or after the exclusion date |

Rules are not mutually exclusive. A large first year at an ordinary rate can also trip velocity. Beneficiary counts are not added across months.

## Language

Use anomaly, indicator, peer deviation, or review lead. Do not describe a provider as a fraudster, do not call a row guilty, and do not label a peer-gap figure as a fraud amount. There is no "mark as fraud" action.

## Develop

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
.venv/bin/python -m unittest discover -s tests -v
```

The build notes are in [docs/build-plan.md](docs/build-plan.md).
