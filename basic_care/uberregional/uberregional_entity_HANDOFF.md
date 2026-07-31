# Handoff: `bas_firms.uberregional_entity`

Reference for working with the überregional (cross-regional) client entity table.
Built by [cross-regional.ipynb](cross-regional.ipynb).

## What this table is

One row per **legal entity / location** belonging to a cross-regional ("überregional")
client, consolidated from a Google Sheet plus the `cross_regional_firms` table. Each row
is linked to an **accounting collection** via the `(mother_id, padoa_id)` couple.

- **Location:** PostgreSQL `medisoft` DB, schema `bas_firms`, table `uberregional_entity`
- **Rows:** 528 · **collections (distinct mother_id):** 96 · **client groups:** 57
- Reachable through DuckDB: `merge_tables.db.connection.connect_to_postgres_via_duckdb()`
  attaches Postgres as `pg`, so query it as `pg.bas_firms.uberregional_entity`.

## Schema

| column | type | meaning |
|---|---|---|
| `client_group` | VARCHAR | Which client the entity belongs to. For tab-sourced rows = the sheet tab name; for auto-generated rows = master sheet `client_group`. |
| `source` | VARCHAR | Origin system: `basic_care` (355), `easybill` (97), `medisoft` (76). |
| `source_id` | VARCHAR | The entity's id in `source` (kept as text — leading zeros / mixed formats). |
| `name` | VARCHAR | Entity name. |
| `standort` | VARCHAR | Location/city (Berlin, München, …); may be NULL. |
| `address` | VARCHAR | Full address; may contain `\n`. |
| `mother_id` | VARCHAR | padoa id of the collection root this entity belongs to. NULL only when there was no id to link (10 rows, all blank `padoa_id`). |
| `padoa_id` | VARCHAR | The entity's own padoa id: a real numeric id, or a `new_<n>` placeholder, or NULL. |
| `needs_review` | BOOLEAN | TRUE when the collection link couldn't be resolved automatically. **Currently 0.** |
| `acc_coll_mother` | BOOLEAN | Entity is the mother of an accounting collection (82 rows). |
| `acc_coll_daughter` | BOOLEAN | Entity is a daughter in an accounting collection (324 rows). |
| `simple_coll` | BOOLEAN | Simple collection (32 rows). |
| `admin_shell` | BOOLEAN | Admin shell for Basic-Care invoicing (25 rows). |
| `comment` | VARCHAR | Free-text note from the sheet. |
| `loaded_at` | TIMESTAMPTZ | When the row was written. |

## The `(mother_id, padoa_id)` couple

This pair links an entity to its accounting collection. `mother_id` = the collection root's
padoa id; `padoa_id` = the entity's own id within it. Derivation from the sheet's raw
`padoa_id` string:

| raw value | `mother_id` | `padoa_id` |
|---|---|---|
| `130000478_new_3` (`<id>_new_n`) | `130000478` (prefix) | `new_3` |
| `113010031` (pure numeric) | `113010031` (itself — a formatted id is its own mother) | `113010031` |
| `new_1` (placeholder, no id) | inferred tab mother | `new_1` |
| blank | NULL | NULL |
| `n/a`, `new`, id-lists | NULL | raw value, `needs_review=TRUE` |

A tab's mother is the row whose `padoa_id` is pure-numeric AND `acc_coll_mother` or
`admin_shell` is TRUE; used only to resolve `new_<n>` rows.

## Data provenance (two populations)

1. **Tab-sourced (385 rows)** — from 36 per-client tabs in the Google Sheet
   (`SPREADSHEET_ID = 1ETOJv6UCQP8Azj_v83KZKjwPZibXkTGgC0UvTTUG5hY`). These are the clients
   that needed manual mapping. **`EJF_IS` and `Medicover_IS` tabs are deliberately excluded.**
   Header labels drift across tabs and are normalized; template-scaffolding rows are dropped.

2. **Auto-generated (143 rows)** — the 21 clients from the master `uberregional kunde` tab with
   `need_manual_check = FALSE` (all have `accounting_collection = TRUE`). For each, entities come
   from `bas_firms.cross_regional_firms` joined on `easybill ID`:
   - **mother row** per easybill ID: `source='easybill'`, `acc_coll_mother=TRUE`,
     `mother_id = padoa_id = source_id =` the easybill ID, name from the master sheet.
   - **daughter rows**: `source='basic_care'`, `acc_coll_daughter=TRUE`, `mother_id =` easybill ID,
     `padoa_id = 'new_<n>'` numbered per collection (ordered by Wochenliste ID), name/standort/address
     from `cross_regional_firms` (`Kunde` / `BAS Standort` / `Anschrift`), `source_id =` Wochenliste ID.

## Known caveats

- `mother_id`/`padoa_id` are **VARCHAR**, not FKs — no referential integrity is enforced.
- 10 rows have NULL `padoa_id`/`mother_id` (medisoft/easybill rows with no id in the sheet).
- Auto-generated `new_<n>` padoa ids are **placeholders**, not real padoa ids yet.
- The mother rows for auto-generated collections use `source='easybill'`; their daughters use
  `basic_care`. (Convention choice, not derived from a source system.)
- Rebuilding the table (`CREATE OR REPLACE` in the notebook) then re-running the append cell
  reproduces all 528 rows; the append step is idempotent (delete-then-insert on the 21 mother_ids).

## Handy queries

```sql
-- every entity in one collection
SELECT * FROM bas_firms.uberregional_entity WHERE mother_id = '113010031';

-- collection sizes
SELECT mother_id, count(*) FILTER (WHERE acc_coll_daughter) AS daughters
FROM bas_firms.uberregional_entity GROUP BY mother_id ORDER BY daughters DESC;

-- rows still needing a padoa id (placeholders)
SELECT client_group, name, standort, mother_id, padoa_id
FROM bas_firms.uberregional_entity WHERE padoa_id LIKE 'new\_%';
```
