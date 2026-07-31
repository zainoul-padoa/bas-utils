"""
Write step: INSERT new (easybill_id, medisoft_id) couples into
pg.bas_firms.easybill_medisoft with id left empty (NULL).

Reads ./output/easybill_couples_new.csv produced by prep_easybill_couples.py.
Idempotent: re-computes which rows are still missing before inserting, and
runs inside a transaction after taking a timestamped backup.

Usage:
    uv run python write_easybill_couples.py            # dry run (default)
    uv run python write_easybill_couples.py --apply     # actually write
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from update_from_sheet import _sql_literal
from merge_tables.db.connection import connect_to_postgres_via_duckdb

TARGET_TABLE = "pg.bas_firms.easybill_medisoft"
NEW_CSV = SCRIPT_DIR / "output" / "easybill_couples_new.csv"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Actually write (default is dry-run)")
    args = parser.parse_args()

    if not NEW_CSV.is_file():
        raise FileNotFoundError(f"Missing {NEW_CSV}. Run prep_easybill_couples.py first.")

    duck = connect_to_postgres_via_duckdb()

    duck.execute(
        f"""
        CREATE OR REPLACE TABLE to_insert AS
        SELECT easybill_id, NULLIF(medisoft_id, '') AS medisoft_id
        FROM read_csv('{_sql_literal(str(NEW_CSV))}', header=true, all_varchar=true)
        """
    )
    n_csv = duck.sql("SELECT count(*) FROM to_insert").fetchone()[0]

    # Re-filter against the live table so the write stays idempotent.
    duck.execute(
        f"""
        CREATE OR REPLACE TABLE to_insert_missing AS
        SELECT t.easybill_id, t.medisoft_id
        FROM to_insert t
        LEFT JOIN {TARGET_TABLE} x
          ON t.easybill_id = x.easybill_id
         AND COALESCE(t.medisoft_id, '') = COALESCE(x.medisoft_id, '')
        WHERE x.easybill_id IS NULL
        """
    )
    n_missing = duck.sql("SELECT count(*) FROM to_insert_missing").fetchone()[0]

    print("=" * 80)
    print("INSERT new couples into", TARGET_TABLE)
    print("=" * 80)
    print(f"  Couples in CSV: {n_csv}")
    print(f"  Still missing from table (will insert): {n_missing}")
    duck.sql("SELECT * FROM to_insert_missing ORDER BY easybill_id").show(max_width=120, max_rows=50)

    if not args.apply:
        print("\n[DRY RUN] Nothing written. Re-run with --apply to insert.")
        return

    if n_missing == 0:
        print("\nNothing to insert. Done.")
        return

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = f"pg.bas_firms.easybill_medisoft_backup_{ts}"
    print(f"\nCreating backup {backup} ...")
    duck.execute(f"CREATE TABLE {backup} AS SELECT * FROM {TARGET_TABLE}")
    nb = duck.sql(f"SELECT count(*) FROM {backup}").fetchone()[0]
    print(f"  ✓ Backed up {nb} rows")

    before = duck.sql(f"SELECT count(*) FROM {TARGET_TABLE}").fetchone()[0]
    duck.execute("BEGIN;")
    try:
        inserted = duck.execute(
            f"""
            INSERT INTO {TARGET_TABLE} (id, easybill_id, medisoft_id)
            SELECT NULL, easybill_id, medisoft_id FROM to_insert_missing
            """
        ).rowcount
        duck.execute("COMMIT;")
    except Exception as e:
        duck.execute("ROLLBACK;")
        print(f"  ✗ ERROR (rolled back): {e}")
        raise

    after = duck.sql(f"SELECT count(*) FROM {TARGET_TABLE}").fetchone()[0]
    print(f"  ✓ Inserted {inserted} rows ({before} -> {after})")
    print("\n✓ DONE.")


if __name__ == "__main__":
    main()
