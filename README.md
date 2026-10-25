# Fraud Ledger

Local review workstation for public healthcare billing anomalies. It loads public extracts, scores peer deviations, and writes a report packet you can file yourself. It does not decide that anyone committed fraud, and it does not submit a tip.

Statistical flags are review leads, not findings of fraud. Excess dollars are a gap versus a peer median, not a loss amount and not a recoverable amount. A person decides whether any lead is worth sending to [america.gov/fraud](https://america.gov/fraud). The Department of Justice Criminal Tip Rewards Pilot is discretionary. This program does not compute a reward.

The review UI binds to `127.0.0.1:8088`. It is an analyst tool for one desktop, not a public website. Open `http://127.0.0.1:8088/index` for the score formula, the ownership role codes, and the CMS rule for totaling a percent.

## Install

One Ubuntu Desktop machine, 24.04 LTS or 26.04 LTS:

```bash
git clone https://github.com/jhenrysec/fraudledger.git
cd fraudledger
bash install.sh
```

That uses sudo once. It installs Python packages into `/opt/fraudledger/venv`, creates a `fraudledger` system user, writes `/etc/fraudledger/config.yaml` if it is missing, and starts a systemd service. It does not load billing data. Open `http://127.0.0.1:8088/sources` and choose **Load the public working set**. Wayland versus X11 does not matter; this is a local web page.

Ubuntu 26.04 Desktop's default Python is 3.14. The installer uses that interpreter and installs a DuckDB build that already has a 3.14 wheel, so it does not compile a database engine on the machine.

```bash
bash install.sh --user       # virtualenv in this clone, no systemd
bash install.sh --no-start   # install services but do not start the UI
```

The public working set is the review path. It loads the OIG exclusion list, hospice and home-health enrollments and owners, NPPES when the CMS index is online, the latest Medicare hospice and home-health provider files, Hospice Care Compare, Home Health Care Compare, and the Medicaid Provider Spending extract. Only hospice, home health, and personal-care procedure codes are written into the warehouse. Monthly patient counts are kept. A payment jump that tracks the patient count, or that leaves paid per patient almost unchanged, is not flagged. A provider that bills only the expected codes for hospice (and has a hospice taxonomy), home health, or personal care is not flagged for a narrow or flat pattern. A cost outlier is compared with other billers in the provider's state when that state has at least 30. Otherwise the comparison is national. An incorporation date, when the enrollment file has one, is used before the NPPES date. A missing date is not treated as a new organization. An owner matches the exclusion list on first name, last name, and state, and on middle name when both sides have one. A reinstated person is not flagged. The agency name is not matched. Care Compare live discharges and home-health readmissions are corroborating indicators. They are not a Critical tier by themselves. HCRIS cost reports, SAM.gov, and state Medicaid exclusion lists are not loaded. The download is still the full multi-gigabyte extract, so leave a large free disk and at least 16 GB of RAM. A smaller machine can load the LEIE and the owner files.

The `--user` command wires `.venv/bin/fraudledger` at this clone's `var/config.yaml`. The system command uses `/etc/fraudledger/config.yaml` and does not depend on the clone staying on disk.

Re-running the installer does not overwrite an existing config file. Public extracts are kept. Starting the review UI also drops leftover rows that are not from a public extract.

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
fraudledger ingest medicaid --path spending.csv
fraudledger ingest medicaid --url "$MEDICAID_URL"
fraudledger ingest leie --path UPDATED.csv
fraudledger ingest nppes --path NPPES_Data_Dissemination.zip
fraudledger ingest ownership --path hospice_owners.csv --kind hospice_owners
fraudledger run
fraudledger verify-run 20261021T120000Z
fraudledger export-tip 20261021T120000Z
fraudledger review 1234567890 --state watch --note "Compare to the enrollment file"
fraudledger serve
fraudledger sources
fraudledger pull leie
fraudledger pull hospice
fraudledger pull --working
fraudledger pull medicaid --focus priority
fraudledger pull --url "$FILE_URL" --as medicaid
fraudledger pull --all
fraudledger refresh
fraudledger report 1234567890 --run 20261021T120000Z
```

`fraudledger pull --working` is the same action as **Load the public working set**. The Sources page shows percent complete for the file being downloaded. Queue stays usable during the download. CMS enrollment and owner files are Windows-1252.

The Medicaid extract is a Parquet file of about 3 GB. The loader recognizes it by its file header, including when the saved name does not end in `.parquet`. A completed download is reused when its size still matches the server, so a failed read does not make you download it again.

`fraudledger report` writes `exports/<run>-<provider>.zip` with `narrative.txt`, `evidence.csv`, `indicators.csv`, `how-to-submit.txt`, and `report.pdf`. The PDF opens with the bottom line, then the dollar figures (paid in the extract, and dollars above a typical peer), then the monthly chart with the peer median, then a plain-language explanation of each indicator. The provider page uses that same text, including how the score is added up, the incorporation date, and the other billing NPIs on a shared-clinician indicator. A payment jump is counted once. You attach the PDF yourself at [america.gov/fraud](https://america.gov/fraud). The command does not contact the government. Do not type the peer-gap figure into the form as money taken.

`fraudledger sources` lists the known public files. `pull` looks up the current download for that file. `pull --url` uses an address you paste, and `--as` says how to read it (`medicaid`, `leie`, `nppes`, `hospice-owners`, `hospice-enrollments`, `home-health-owners`, `home-health-enrollments`). `--focus priority` keeps hospice, home health, and personal-care procedure codes from a Medicaid file. The same choices are on the Sources page at `http://127.0.0.1:8088/sources`.

Hospice and home health pulls also load the matching enrollment file. The owner extract is keyed by enrollment id; the enrollment extract supplies the NPI, which is what the billing rows use.

`refresh` still downloads only the URLs saved in the config. If every URL is empty, it exits 0 and downloads nothing. Put a URL in `sources:` when you want the monthly timer to fetch that file instead of looking it up. The timer runs on the 3rd day of the month at 02:00.

`pull --all` fetches every known source, including the multi-gigabyte Medicaid extract. It is not what the timer runs.

`export-tip` zips the whole run for your notes. `report` is the per-provider packet. Neither contacts the government.

## Public sources

Set these in `/etc/fraudledger/config.yaml` only when you want `refresh` to use a fixed URL. Leave them empty to let `fraudledger pull` find the current file.

| Source | Where the pull looks |
| --- | --- |
| Medicaid Provider Spending | https://opendata.hhs.gov/datasets/medicaid-provider-spending/ |
| NPPES monthly file | https://download.cms.gov/nppes/NPI_Files.html |
| OIG LEIE | https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv |
| Hospice owners and enrollments | https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/hospice-all-owners |
| Home health owners and enrollments | https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/home-health-agency-all-owners |
| Medicare hospice and home health use | CMS data catalog, latest post-acute provider CSV |
| Hospice Care Compare | CMS provider-data catalog, title "Hospice - Provider Data" |
| Home health Care Compare | CMS provider-data catalog, title "Home Health Care Agencies" |

v1 scores hospice, home health, and personal-care procedure codes in the Medicaid spending extract, plus an NPI match to the LEIE. The public working set does not keep the rest of the national extract. Owner linking uses the CMS associate id only. A shared address is an extra edge when 3 to 25 providers use the same street, city, and state. A post-office box is not linked. The cluster bonus is not added inside a group of more than 40 providers. Name-only LEIE matches are not flags. An agency name is not matched to the exclusion list.

The national Medicaid extract is on the order of 227 million rows. Do not open it in pandas. The loader streams it through DuckDB and writes only the priority procedure codes. The raw download is still stored, so plan on tens of gigabytes of disk before starting the working set.

## Rules

Thresholds live in the YAML config and the hash of that file is stored on the run.

| Rule | Lead |
| --- | --- |
| R01 | Paid per line above 3× the state HCPCS median when that state has 30 billers, otherwise the national median; at least $100k |
| R02 | Above the 90th percentile rate on two or more codes, using the same state-or-national peer group |
| R03 | Lines divided by the busiest month's beneficiary count, over 100 |
| R04 | Lines per working day at least 50 |
| R05 | One or two codes and at least $1M paid, unless the codes are one expected family |
| R06 | Coefficient of variation of monthly paid under 0.10, six months, $250k, with the same family exemption |
| R07 | Year-over-year growth over 2 and an absolute change of at least $1M, unless the patient count or the paid-per-patient rate explains it |
| R08 | Year-over-year growth over 5 and latest-year paid at least $250k, with the same growth exceptions |
| R09 | First-year paid at least $1M and a real incorporation or enumeration date says the organization is new |
| R10 | Incorporation, or enumeration when incorporation is missing, on or after 2022-01-01 and lifetime paid at least $5M |
| R11 | Billing NPI is on the LEIE and paid amounts fall on or after the exclusion date |
| R12 | A payment jump with a qualifying owner associated in that year or the year before. The dollar increase is the one on R07 and is not added again |
| R13 | Owner matches the LEIE on first name, last name, and state. Middle name must agree when both sides have one. A reinstated person is not flagged |
| R14 | Medicare hospice days per stay above 2× the provider median, CCN plus state when both files have a state |
| R15 | Medicare home health visits per episode above 2× the provider median |
| R16 | A servicing NPI other than the biller is on the LEIE, with payment on or after the exclusion date |
| R17 | The same servicing NPI is paid through at least 3 billing NPIs that do not share an owner, and at least $250k. The other billing NPIs are named |
| R18 | Care Compare early or late live discharges above 2× the hospice median, at least 8 peers |
| R19 | Care Compare potentially preventable readmissions above 2× the home-health median, at least 8 peers |

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
