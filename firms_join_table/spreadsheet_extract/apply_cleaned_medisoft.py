"""
Insert NEW cleaned_medisoft rows (from prep_cleaned_medisoft.py) into
pg.bas_firms.cleaned_medisoft.

Insert-only: existing medisoft_ids are never modified. Idempotent (re-filters
against the live table), takes a timestamped backup, runs in one transaction.

Usage:
    uv run python apply_cleaned_medisoft.py            # dry run (default)
    uv run python apply_cleaned_medisoft.py --apply     # actually write
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

TARGET = "pg.bas_firms.cleaned_medisoft"
NEW_CSV = SCRIPT_DIR / "output" / "cleaned_medisoft_new.csv"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Actually write (default dry-run)")
    args = parser.parse_args()

    if not NEW_CSV.is_file():
        raise FileNotFoundError(f"Missing {NEW_CSV}. Run prep_cleaned_medisoft.py first.")

    d = connect_to_postgres_via_duckdb()
    d.execute(
        f"""
        CREATE OR REPLACE TABLE staged AS
        SELECT
            NULLIF(TRIM(medisoft_id), '')          AS medisoft_id,
            NULLIF(name, '')                        AS name,
            NULLIF(kuerzel, '')                     AS kuerzel,
            NULLIF(pfad, '')                        AS pfad,
            TRY_CAST(NULLIF(nb_patients, '') AS INTEGER) AS nb_patients,
            NULLIF(last_exam_date, '')              AS last_exam_date,
            NULLIF(address, '')                     AS address,
            TRY_CAST(NULLIF(has_easybill_connection, '') AS BOOLEAN) AS has_easybill_connection,
            TRY_CAST(NULLIF(migrate_as_inactive, '') AS BOOLEAN)     AS migrate_as_inactive,
            TRY_CAST(NULLIF(no_migration, '') AS BOOLEAN)            AS no_migration,
            TRY_CAST(NULLIF(selbstzahler, '') AS BOOLEAN)            AS selbstzahler,
            NULLIF(city, '')                        AS city
        FROM read_csv('{_sql_literal(str(NEW_CSV))}', header=true, all_varchar=true)
        WHERE NULLIF(TRIM(medisoft_id), '') IS NOT NULL
        """
    )
    # Idempotent: only medisoft_ids still absent from the live table.
    d.execute(
        f"""
        CREATE OR REPLACE TABLE to_insert AS
        SELECT s.* FROM staged s
        LEFT JOIN {TARGET} t ON s.medisoft_id = t.medisoft_id
        WHERE t.medisoft_id IS NULL
        """
    )
    n_csv = d.sql("SELECT count(*) FROM staged").fetchone()[0]
    n_ins = d.sql("SELECT count(*) FROM to_insert").fetchone()[0]
    before = d.sql(f"SELECT count(*) FROM {TARGET}").fetchone()[0]

    print("=" * 70)
    print("INSERT new rows into", TARGET)
    print("=" * 70)
    print(f"  Current rows:               {before}")
    print(f"  Staged from CSV:            {n_csv}")
    print(f"  Still missing (to insert):  {n_ins}")
    print(f"  Projected final rows:       {before + n_ins}")
    d.sql("SELECT city, count(*) FROM to_insert GROUP BY city ORDER BY city").show()

    if not args.apply:
        print("\n[DRY RUN] Nothing written. Re-run with --apply.")
        return
    if n_ins == 0:
        print("\nNothing to insert.")
        return

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = f"pg.bas_firms.cleaned_medisoft_backup_{ts}"
    print(f"\nCreating backup {backup} ...")
    d.execute(f"CREATE TABLE {backup} AS SELECT * FROM {TARGET}")
    print(f"  ✓ Backed up {d.sql(f'SELECT count(*) FROM {backup}').fetchone()[0]} rows")

    d.execute("BEGIN;")
    try:
        inserted = d.execute(
            f"""
            INSERT INTO {TARGET}
            (medisoft_id, name, kuerzel, pfad, nb_patients, last_exam_date, address,
             has_easybill_connection, migrate_as_inactive, no_migration, selbstzahler, city)
            SELECT medisoft_id, name, kuerzel, pfad, nb_patients, last_exam_date, address,
                   has_easybill_connection, migrate_as_inactive, no_migration, selbstzahler, city
            FROM to_insert
            """
        ).rowcount
        d.execute("COMMIT;")
    except Exception as e:
        d.execute("ROLLBACK;")
        print(f"  ✗ ERROR (rolled back): {e}")
        raise

    after = d.sql(f"SELECT count(*) FROM {TARGET}").fetchone()[0]
    print(f"  ✓ Inserted {inserted} rows ({before} -> {after})")
    print(f"  Backup retained: {backup}")
    print("\n✓ DONE.")


if __name__ == "__main__":
    main()
