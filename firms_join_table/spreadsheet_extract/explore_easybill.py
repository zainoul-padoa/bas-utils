"""Read-only exploration: easybill_clients_list sheet + target table easybill_medisoft."""
from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from update_from_sheet import connect_gsheets, DEFAULT_CREDENTIALS
from merge_tables.db.connection import connect_to_postgres_via_duckdb

SPREADSHEET_ID = "1vB84YG3eBVJVAQsv8VNe2K_TTE8bZ1I9gnwidGAvyXE"
SHEET_NAME = "easybill_clients_list"


def main() -> None:
    print("=" * 80)
    print("SHEET EXPLORATION")
    print("=" * 80)
    ds = connect_gsheets(DEFAULT_CREDENTIALS, database=":memory:")
    ds.execute(
        f"""
        CREATE OR REPLACE TABLE sheet_data AS
        SELECT * FROM read_gsheet('{SPREADSHEET_ID}', sheet='{SHEET_NAME}', all_varchar=true)
        """
    )
    print("\nColumns:")
    for row in ds.sql("DESCRIBE sheet_data").fetchall():
        print(f"  - {row[0]}: {row[1]}")
    n = ds.sql("SELECT count(*) FROM sheet_data").fetchone()[0]
    print(f"\nRow count: {n}")
    print("\nFirst 10 rows:")
    ds.sql("SELECT * FROM sheet_data LIMIT 10").show(max_width=200, max_rows=20)

    print("\n" + "=" * 80)
    print("TARGET TABLE: pg.bas_firms.easybill_medisoft")
    print("=" * 80)
    duck = connect_to_postgres_via_duckdb()
    print("\nColumns:")
    for row in duck.sql("DESCRIBE pg.bas_firms.easybill_medisoft").fetchall():
        print(f"  {row[0]:<25} {row[1]}")
    tn = duck.sql("SELECT count(*) FROM pg.bas_firms.easybill_medisoft").fetchone()[0]
    print(f"\nRow count: {tn}")
    print("\nSample rows:")
    duck.sql("SELECT * FROM pg.bas_firms.easybill_medisoft LIMIT 10").show(max_width=200)


if __name__ == "__main__":
    main()
