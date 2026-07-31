"""
LOCAL-ONLY reconcile of pg.bas_firms.easybill_medisoft against the
`easybill_clients_list` sheet.

The sheet is treated as source-of-truth ONLY for the easybill_ids it lists.
Table rows for easybill_ids absent from the sheet are left untouched.

Desired end-state for an in-scope easybill_id `eb`:
  - every real (eb, medisoft_id) couple listed in the sheet, PLUS
  - a single (eb, NULL) placeholder row iff the sheet gives `eb` no real medisoft.

From that we compute, scoped to sheet easybill_ids:
  - ADD    = desired couples missing from the table
  - DELETE = table couples not in desired (removed / changed links)

DELETEs are split into:
  - real-link removals (table had a real medisoft_id no longer in the sheet)  <-- the risky ones
  - NULL-placeholder cleanups (e.g. (eb, NULL) row replaced by a real mapping)

Writes ./output/easybill_reconcile_add.csv and easybill_reconcile_delete.csv.
Does NOT write to the database.
"""
from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from update_from_sheet import connect_gsheets, DEFAULT_CREDENTIALS, _sql_literal
from merge_tables.db.connection import connect_to_postgres_via_duckdb

SPREADSHEET_ID = "1vB84YG3eBVJVAQsv8VNe2K_TTE8bZ1I9gnwidGAvyXE"
SHEET_NAME = "easybill_clients_list"
TARGET_TABLE = "pg.bas_firms.easybill_medisoft"
OUTPUT_DIR = SCRIPT_DIR / "output"


def main() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)

    # --- 1. Read sheet, build desired couple set (LOCAL) -----------------
    ds = connect_gsheets(DEFAULT_CREDENTIALS, database=":memory:")
    ds.execute(
        f"""
        CREATE OR REPLACE TABLE sheet_data AS
        SELECT * FROM read_gsheet(
            '{_sql_literal(SPREADSHEET_ID)}', sheet='{_sql_literal(SHEET_NAME)}', all_varchar=true)
        """
    )
    ds.execute(
        """
        CREATE OR REPLACE TABLE couples AS
        WITH parsed AS (
            SELECT
                TRIM(easybill_kundennummer) AS easybill_id,
                TRIM(UNNEST(STRING_SPLIT(
                    COALESCE(NULLIF(TRIM(medisoft_ids), ''), ''), CHR(10)))) AS medisoft_id
            FROM sheet_data
            WHERE NULLIF(TRIM(easybill_kundennummer), '') IS NOT NULL
        )
        SELECT DISTINCT easybill_id, NULLIF(medisoft_id, '') AS medisoft_id FROM parsed
        """
    )
    # desired = real couples + (eb, NULL) for eb with no real medisoft
    ds.execute(
        """
        CREATE OR REPLACE TABLE desired AS
        SELECT DISTINCT easybill_id, medisoft_id
        FROM couples WHERE medisoft_id IS NOT NULL
        UNION ALL
        SELECT DISTINCT easybill_id, CAST(NULL AS VARCHAR) AS medisoft_id
        FROM couples
        WHERE easybill_id NOT IN (
            SELECT easybill_id FROM couples WHERE medisoft_id IS NOT NULL
        )
        """
    )
    desired_csv = OUTPUT_DIR / "_desired.csv"
    ds.execute(f"COPY desired TO '{_sql_literal(str(desired_csv))}' (HEADER, DELIMITER ',')")
    sheet_eb = ds.sql("SELECT count(DISTINCT easybill_id) FROM couples").fetchone()[0]
    n_desired = ds.sql("SELECT count(*) FROM desired").fetchone()[0]

    # --- 2. Load into PG connection & diff, scoped to sheet easybill_ids --
    duck = connect_to_postgres_via_duckdb()
    duck.execute(
        f"""
        CREATE OR REPLACE TABLE desired AS
        SELECT easybill_id, NULLIF(medisoft_id, '') AS medisoft_id
        FROM read_csv('{_sql_literal(str(desired_csv))}', header=true, all_varchar=true)
        """
    )
    duck.execute("CREATE OR REPLACE TABLE sheet_eb AS SELECT DISTINCT easybill_id FROM desired")

    # table couples for in-scope easybill_ids (deduped)
    duck.execute(
        f"""
        CREATE OR REPLACE TABLE table_scope AS
        SELECT DISTINCT t.easybill_id, t.medisoft_id
        FROM {TARGET_TABLE} t
        JOIN sheet_eb s ON t.easybill_id = s.easybill_id
        """
    )

    # ADD = desired not in table_scope
    duck.execute(
        """
        CREATE OR REPLACE TABLE to_add AS
        SELECT d.easybill_id, d.medisoft_id
        FROM desired d
        LEFT JOIN table_scope t
          ON d.easybill_id = t.easybill_id
         AND COALESCE(d.medisoft_id,'') = COALESCE(t.medisoft_id,'')
        WHERE t.easybill_id IS NULL
        """
    )
    # DELETE = table_scope not in desired
    duck.execute(
        """
        CREATE OR REPLACE TABLE to_delete AS
        SELECT t.easybill_id, t.medisoft_id
        FROM table_scope t
        LEFT JOIN desired d
          ON t.easybill_id = d.easybill_id
         AND COALESCE(t.medisoft_id,'') = COALESCE(d.medisoft_id,'')
        WHERE d.easybill_id IS NULL
        """
    )

    n_scope = duck.sql("SELECT count(*) FROM table_scope").fetchone()[0]
    n_add = duck.sql("SELECT count(*) FROM to_add").fetchone()[0]
    n_del = duck.sql("SELECT count(*) FROM to_delete").fetchone()[0]
    n_del_real = duck.sql("SELECT count(*) FROM to_delete WHERE medisoft_id IS NOT NULL").fetchone()[0]
    n_del_null = n_del - n_del_real
    n_add_real = duck.sql("SELECT count(*) FROM to_add WHERE medisoft_id IS NOT NULL").fetchone()[0]

    print("=" * 80)
    print("RECONCILE easybill_medisoft vs sheet (LOCAL, read-only)")
    print("=" * 80)
    print(f"  easybill_ids in sheet (scope):          {sheet_eb}")
    print(f"  desired couples:                        {n_desired}")
    print(f"  table couples in scope:                 {n_scope}")
    print(f"  --")
    print(f"  ADD couples:    {n_add}   (with real medisoft: {n_add_real}, NULL placeholders: {n_add - n_add_real})")
    print(f"  DELETE couples: {n_del}   (REAL-link removals: {n_del_real}, NULL cleanups: {n_del_null})")

    print("\n  >>> DELETE — REAL medisoft-link removals (the risky ones):")
    duck.sql("SELECT * FROM to_delete WHERE medisoft_id IS NOT NULL ORDER BY easybill_id").show(max_width=120, max_rows=100)
    print("\n  >>> DELETE — NULL-placeholder cleanups:")
    duck.sql("SELECT * FROM to_delete WHERE medisoft_id IS NULL ORDER BY easybill_id").show(max_width=120, max_rows=100)
    print("\n  >>> ADD — with real medisoft_id:")
    duck.sql("SELECT * FROM to_add WHERE medisoft_id IS NOT NULL ORDER BY easybill_id").show(max_width=120, max_rows=100)
    print(f"  (+ {n_add - n_add_real} ADD rows that are (easybill_id, NULL) placeholders for new clients)")

    duck.execute(f"COPY to_add TO '{_sql_literal(str(OUTPUT_DIR / 'easybill_reconcile_add.csv'))}' (HEADER, DELIMITER ',')")
    duck.execute(f"COPY to_delete TO '{_sql_literal(str(OUTPUT_DIR / 'easybill_reconcile_delete.csv'))}' (HEADER, DELIMITER ',')")
    desired_csv.unlink(missing_ok=True)
    print("\n  ✓ Wrote output/easybill_reconcile_add.csv and easybill_reconcile_delete.csv")
    print("\nDONE (local only — no DB writes).")


if __name__ == "__main__":
    main()
