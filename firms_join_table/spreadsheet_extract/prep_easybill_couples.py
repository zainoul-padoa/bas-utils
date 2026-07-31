"""
LOCAL-ONLY prep for updating pg.bas_firms.easybill_medisoft.

Reads the `easybill_clients_list` sheet, flattens (easybill_id, medisoft_id)
couples (medisoft_ids is newline-separated), compares against the existing
target table, and writes local CSVs + stats. Does NOT write to the database.

Outputs (in ./output/):
  - easybill_couples_all.csv   : every distinct couple from the sheet
  - easybill_couples_new.csv   : couples not already present in the target table
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

    print("=" * 80)
    print("STEP 1 — Read sheet & flatten couples (LOCAL)")
    print("=" * 80)
    ds = connect_gsheets(DEFAULT_CREDENTIALS, database=":memory:")
    ds.execute(
        f"""
        CREATE OR REPLACE TABLE sheet_data AS
        SELECT * FROM read_gsheet(
            '{_sql_literal(SPREADSHEET_ID)}',
            sheet='{_sql_literal(SHEET_NAME)}',
            all_varchar=true
        )
        """
    )

    # Flatten: one row per (easybill_id, medisoft_id). medisoft_ids is newline
    # separated; empty medisoft -> NULL (couple with no medisoft match).
    ds.execute(
        """
        CREATE OR REPLACE TABLE couples AS
        WITH parsed AS (
            SELECT
                TRIM(easybill_kundennummer) AS easybill_id,
                TRIM(UNNEST(STRING_SPLIT(
                    COALESCE(NULLIF(TRIM(medisoft_ids), ''), ''),
                    CHR(10)
                ))) AS medisoft_id
            FROM sheet_data
            WHERE NULLIF(TRIM(easybill_kundennummer), '') IS NOT NULL
        )
        SELECT DISTINCT
            easybill_id,
            NULLIF(medisoft_id, '') AS medisoft_id
        FROM parsed
        """
    )

    n_all = ds.sql("SELECT count(*) FROM couples").fetchone()[0]
    n_eb = ds.sql("SELECT count(DISTINCT easybill_id) FROM couples").fetchone()[0]
    n_with = ds.sql("SELECT count(*) FROM couples WHERE medisoft_id IS NOT NULL").fetchone()[0]
    n_without = ds.sql("SELECT count(*) FROM couples WHERE medisoft_id IS NULL").fetchone()[0]
    print(f"  Distinct couples: {n_all}")
    print(f"  Distinct easybill_ids: {n_eb}")
    print(f"  Couples WITH medisoft_id: {n_with}")
    print(f"  Couples WITHOUT medisoft_id (NULL): {n_without}")
    print("\n  Sample couples:")
    ds.sql("SELECT * FROM couples ORDER BY easybill_id LIMIT 15").show(max_width=120)

    all_csv = OUTPUT_DIR / "easybill_couples_all.csv"
    ds.execute(f"COPY couples TO '{_sql_literal(str(all_csv))}' (HEADER, DELIMITER ',')")
    print(f"\n  ✓ Wrote all couples -> {all_csv}")

    print("\n" + "=" * 80)
    print("STEP 2 — Compare against existing target table (READ-ONLY)")
    print("=" * 80)
    duck = connect_to_postgres_via_duckdb()

    # Pull existing couples from PG into local duckdb-sheets connection via CSV
    existing_csv = OUTPUT_DIR / "_existing_pairs.csv"
    duck.execute(
        f"""
        COPY (
            SELECT DISTINCT easybill_id, medisoft_id
            FROM {TARGET_TABLE}
        ) TO '{_sql_literal(str(existing_csv))}' (HEADER, DELIMITER ',')
        """
    )
    n_existing = duck.sql(f"SELECT count(*) FROM {TARGET_TABLE}").fetchone()[0]
    print(f"  Existing rows in target: {n_existing}")

    ds.execute(
        f"""
        CREATE OR REPLACE TABLE existing_pairs AS
        SELECT DISTINCT easybill_id, medisoft_id
        FROM read_csv('{_sql_literal(str(existing_csv))}', header=true, all_varchar=true)
        """
    )

    # New couples to insert. Two rules:
    #  1. Exact pair (easybill_id, medisoft_id) not already present, AND
    #  2. A (eb, NULL) couple is only inserted if easybill_id is entirely absent
    #     from the table — otherwise it would add a junk NULL row next to an
    #     existing real mapping. Couples with a real medisoft_id are unaffected.
    ds.execute(
        """
        CREATE OR REPLACE TABLE existing_easybill AS
        SELECT DISTINCT easybill_id FROM existing_pairs
        WHERE easybill_id IS NOT NULL
        """
    )
    ds.execute(
        """
        CREATE OR REPLACE TABLE couples_new AS
        SELECT c.easybill_id, c.medisoft_id
        FROM couples c
        LEFT JOIN existing_pairs e
          ON c.easybill_id = e.easybill_id
         AND COALESCE(c.medisoft_id, '') = COALESCE(e.medisoft_id, '')
        LEFT JOIN existing_easybill ee
          ON c.easybill_id = ee.easybill_id
        WHERE e.easybill_id IS NULL
          AND NOT (c.medisoft_id IS NULL AND ee.easybill_id IS NOT NULL)
        """
    )
    n_new = ds.sql("SELECT count(*) FROM couples_new").fetchone()[0]
    n_new_eb = ds.sql("SELECT count(DISTINCT easybill_id) FROM couples_new").fetchone()[0]
    n_new_with = ds.sql("SELECT count(*) FROM couples_new WHERE medisoft_id IS NOT NULL").fetchone()[0]
    print(f"  NEW couples (not in target): {n_new}  (already present: {n_all - n_new})")
    print(f"    distinct new easybill_ids: {n_new_eb}")
    print(f"    new couples WITH medisoft_id: {n_new_with}")
    print("\n  Sample NEW couples:")
    ds.sql("SELECT * FROM couples_new ORDER BY easybill_id LIMIT 20").show(max_width=120)

    new_csv = OUTPUT_DIR / "easybill_couples_new.csv"
    ds.execute(f"COPY couples_new TO '{_sql_literal(str(new_csv))}' (HEADER, DELIMITER ',')")
    print(f"\n  ✓ Wrote NEW couples -> {new_csv}")

    # cleanup helper file
    existing_csv.unlink(missing_ok=True)

    print("\n" + "=" * 80)
    print("DONE (local only — no DB writes). Review the CSVs above.")
    print("=" * 80)


if __name__ == "__main__":
    main()
