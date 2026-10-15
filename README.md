# bas-utils

Data tooling for the BAS migration. The repo builds one **firm reference** across BAS's three
source systems and stores it in Postgres, schema `bas_firms`:

- **Medisoft**: the medical software (firms, employees, examinations)
- **easybill**: invoicing (contacts, documents, line items)
- **Zoho**: CRM (accounts, contacts, deals)

On top of the matching, SQL scripts generate the Padoa ids (`easybill_medisoft.id`, überregional ids).

## Data flow

```
Sources                                       Matching                                   Target (Postgres)
Medisoft XML ── medisoft/ ──────────► medisoft.*  ┐
easybill CSV ── easybill/update_db/ ► easybill.*  ├─ merge_tables/     Medisoft × Zoho     ┐
Zoho API / CSV ─ zoho/ ─────────────► zoho.*      ┘  firms_join_table/ easybill × Medisoft ├─► bas_firms.*
Google Sheets (city client lists, Wochenliste, überregional) ──────────────────────────────┘   then id SQL
samedi (scraping) ── samedi/ ──► match samedi patients with Medisoft employees                 (medisoft/*.sql, easybill/*.sql)
```

Main target tables: `bas_firms.easybill_medisoft`, `cleaned_medisoft`, `easybill_zoho`,
`full_basic_care`, `basic_care_details`, `uberregional_entity`, `employer_users`, `firm_add_*`.

## Directory map

| Directory | What it does |
|---|---|
| `merge_tables/` | **Shared library, despite the name.** `db/connection.py` (`connect_to_postgres_via_duckdb`: DuckDB with Postgres attached as `pg`) is imported almost everywhere; `db/tables.py` holds the `clean_account_name` macro. Also the first Medisoft × Zoho matcher (`match_firms.py`, name then address, see its README) and `firm_hierarchy/` (Medisoft child firms → easybill mother firms). |
| `medisoft/` | Medisoft XML → SQLite (`xml_to_db.py`) → Postgres (`db.load`, pgloader). SQL: `pre_clean`, duplicate-firm merge, id generation (`generate_easybill_medisoft_id`, `independent_id`), Rostock sectorisation, column stats. |
| `easybill/` | SQL: pre-clean, easybill_id merge across 10 tables (`merge_easybill.sql`), überregional id generation, Zoho Kundennummer link. Notebooks: DB build, active clients, Wochenliste matching. |
| `zoho/` | Zoho API client (`zohoCRM.py` bulk export, `zoho_timeline.py` account history), CSV imports (`import_deals`, `update_tasks`), contacts backup, exploration notebooks. |
| `firms_join_table/` | **Main easybill × Medisoft flow.** `firms_merger` (matching, manual validation sheets) and `list_for_the_client` (per-city client lists). `spreadsheet_extract/`: city sheets → DuckDB → merged couples (`extract.py`), load to Postgres (`load.py`), upsert of `easybill_medisoft` / `cleaned_medisoft` from the `easybill_clients_list` sheet (`update_from_sheet.py`). |
| `samedi/` | Playwright scraper of samedi plannings (`scraping/`), then matching with Medisoft employees. |
| `extract_cdp/` | Standalone: extract codes from visit motives. |
| `postal.ipynb` | Address parsing exploration (deepparse). |
| `connection_alchemy.py` | SQLAlchemy connection, used by older scripts. |

### Not on `main` yet (branch `bas-flux-firm`)

| Directory | What it does |
|---|---|
| `easybill/update_db/` | Upsert of `easybill.contacts` / `easybill.documents` from easybill CSV exports, with a backup table before each write. |
| `firms_join_table/spreadsheet_extract/` (rest) | Step scripts replacing `update_from_sheet.py`: `prep_*` / `reconcile_*` (local CSV only) → `write_*` / `apply_*` (DB write, one transaction, backup); post-Rostock merge of city client lists. |
| `basic_care/` | Basic Care extract and load (`full_basic_care`, `basic_care_details`); `uberregional/` builds `bas_firms.uberregional_entity` (see `uberregional_entity_HANDOFF.md`). |
| `billing/` | Basic Care prices and discounts from easybill invoices (`rebuild_basic_care_discounts.py`), push to the "All clients" sheet, Zoho WZ codes. |
| `employer_user/` | Builds `bas_firms.employer_users` from easybill and a sheet. |
| `firm_add/` | Batches of firms to create (`firm_add_week_*`, Rostock uni, dummy firms). |
| `postal_addresses/`, `sandbox/` | One-off explorations. |

## Setup

```bash
uv sync
cp .env.example .env   # Postgres + Zoho credentials
```

Scripts reading Google Sheets expect a service-account key at `<dir>/config/gsheet-creds.json`
(git-ignored, see the `.exemple` file next to it). Run scripts from the repo root so that
`merge_tables` is importable, e.g. `uv run python firms_join_table/spreadsheet_extract/extract.py`.

Notebooks are tracked **without outputs**: clear them before committing, outputs hold client data.

## Known debt

- Three ways to connect to Postgres: `merge_tables/db/connection.py` (DuckDB, the standard),
  `connection_alchemy.py` (SQLAlchemy), `zoho/db_connection.py` (psycopg2); `zoho/backup_contacts.py`
  and `zoho/update_tasks.py` redefine their own DuckDB connection.
- `config/mapping_spreadsheets.py` is copied as-is in each directory reading sheets (4 identical copies on `bas-flux-firm`).
- `medisoft/README.md` and `zoho/README.md` mention a `requirements.txt` that no longer exists (use `pyproject.toml` / `uv`).
