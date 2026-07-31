"""
Apply the easybill_medisoft reconcile computed by reconcile_easybill_couples.py.

Reads ./output/easybill_reconcile_{add,delete}.csv and applies them to
pg.bas_firms.easybill_medisoft inside a single transaction, after a
timestamped backup:
  - DELETE every (easybill_id, medisoft_id) couple in the delete set
  - INSERT every couple in the add set with id left empty (NULL)

Usage:
    uv run python apply_reconcile_easybill.py            # dry run (default)
    uv run python apply_reconcile_easybill.py --apply     # actually write
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
OUTPUT_DIR = SCRIPT_DIR / "output"
ADD_CSV = OUTPUT_DIR / "easybill_reconcile_add.csv"
DEL_CSV = OUTPUT_DIR / "easybill_reconcile_delete.csv"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Actually write (default dry-run)")
    args = parser.parse_args()

    for p in (ADD_CSV, DEL_CSV):
        if not p.is_file():
            raise FileNotFoundError(f"Missing {p}. Run reconcile_easybill_couples.py first.")

    duck = connect_to_postgres_via_duckdb()
    duck.execute(
        f"""CREATE OR REPLACE TABLE to_add AS
            SELECT easybill_id, NULLIF(medisoft_id,'') AS medisoft_id
            FROM read_csv('{_sql_literal(str(ADD_CSV))}', header=true, all_varchar=true)"""
    )
    duck.execute(
        f"""CREATE OR REPLACE TABLE to_delete AS
            SELECT easybill_id, NULLIF(medisoft_id,'') AS medisoft_id
            FROM read_csv('{_sql_literal(str(DEL_CSV))}', header=true, all_varchar=true)"""
    )
    n_add = duck.sql("SELECT count(*) FROM to_add").fetchone()[0]
    n_del = duck.sql("SELECT count(*) FROM to_delete").fetchone()[0]

    # How many target rows the delete set actually matches (dups counted).
    del_match = duck.sql(
        f"""SELECT count(*) FROM {TARGET_TABLE} t
            WHERE EXISTS (SELECT 1 FROM to_delete d
                          WHERE d.easybill_id = t.easybill_id
                            AND COALESCE(d.medisoft_id,'') = COALESCE(t.medisoft_id,''))"""
    ).fetchone()[0]

    before = duck.sql(f"SELECT count(*) FROM {TARGET_TABLE}").fetchone()[0]
    print("=" * 80)
    print("APPLY RECONCILE to", TARGET_TABLE)
    print("=" * 80)
    print(f"  Current rows:            {before}")
    print(f"  Delete-set couples:      {n_del}  (matches {del_match} table rows)")
    print(f"  Add-set couples:         {n_add}")
    print(f"  Projected final rows:    {before - del_match + n_add}")

    if not args.apply:
        print("\n[DRY RUN] Nothing written. Re-run with --apply.")
        return

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = f"pg.bas_firms.easybill_medisoft_backup_{ts}"
    print(f"\nCreating backup {backup} ...")
    duck.execute(f"CREATE TABLE {backup} AS SELECT * FROM {TARGET_TABLE}")
    print(f"  ✓ Backed up {duck.sql(f'SELECT count(*) FROM {backup}').fetchone()[0]} rows")

    duck.execute("BEGIN;")
    try:
        deleted = duck.execute(
            f"""DELETE FROM {TARGET_TABLE} t
                WHERE EXISTS (SELECT 1 FROM to_delete d
                              WHERE d.easybill_id = t.easybill_id
                                AND COALESCE(d.medisoft_id,'') = COALESCE(t.medisoft_id,''))"""
        ).rowcount
        inserted = duck.execute(
            f"""INSERT INTO {TARGET_TABLE} (id, easybill_id, medisoft_id)
                SELECT NULL, easybill_id, medisoft_id FROM to_add"""
        ).rowcount
        duck.execute("COMMIT;")
    except Exception as e:
        duck.execute("ROLLBACK;")
        print(f"  ✗ ERROR (rolled back): {e}")
        raise

    after = duck.sql(f"SELECT count(*) FROM {TARGET_TABLE}").fetchone()[0]
    print(f"  ✓ Deleted {deleted} rows, inserted {inserted} rows ({before} -> {after})")
    print(f"  Backup retained: {backup}")
    print("\n✓ DONE.")


if __name__ == "__main__":
    main()
