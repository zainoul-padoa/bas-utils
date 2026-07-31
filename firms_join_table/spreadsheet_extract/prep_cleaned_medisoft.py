"""
LOCAL-ONLY prep: find NEW rows for pg.bas_firms.cleaned_medisoft from the
city sheets of the spreadsheet.

Unions all city tabs, maps their columns to the table schema, dedups by
medisoft_id, and keeps only medisoft_ids not already in cleaned_medisoft.
Writes ./output/cleaned_medisoft_new.csv. Does NOT write to the database.
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
OUTPUT_DIR = SCRIPT_DIR / "output"
CITY_SHEETS = [
    "berlin", "dusseldorf", "frankfurt", "hamburg", "kiel",
    "koln", "munchen", "stuttgart", "rostock", "viersen", "other",
]
TARGET_COLS = [
    "medisoft_id", "name", "kuerzel", "pfad", "nb_patients", "last_exam_date",
    "address", "has_easybill_connection", "migrate_as_inactive", "no_migration",
    "selbstzahler", "city",
]


def _bool(col: str) -> str:
    return (
        f"CASE WHEN LOWER({col}) IN ('true','1','yes') THEN TRUE "
        f"WHEN LOWER({col}) IN ('false','0','no') THEN FALSE ELSE NULL END"
    )


def main() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    ds = connect_gsheets(DEFAULT_CREDENTIALS)

    blocks = []
    for city in CITY_SHEETS:
        blocks.append(f"""
            SELECT
                TRIM(medisoft_id) AS medisoft_id,
                name, kuerzel, pfad,
                TRY_CAST(nb_patients AS INTEGER) AS nb_patients,
                last_exam_date,
                addresse AS address,
                {_bool('"has easybill connection"')} AS has_easybill_connection,
                {_bool('"migrate as inactive"')} AS migrate_as_inactive,
                {_bool('"no migration"')} AS no_migration,
                {_bool('"Selbstzahler"')} AS selbstzahler,
                '{_sql_literal(city)}' AS city
            FROM read_gsheet('{_sql_literal(SPREADSHEET_ID)}', sheet='{_sql_literal(city)}', all_varchar=true)
            WHERE NULLIF(TRIM(medisoft_id), '') IS NOT NULL
        """)
    union_sql = " UNION ALL BY NAME ".join(blocks)

    # Dedup by medisoft_id (defensive — a medisoft_id should map to one city).
    ds.execute(f"""
        CREATE TABLE all_city AS
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY medisoft_id ORDER BY city) AS rn
            FROM ({union_sql})
        ) WHERE rn = 1
    """)
    n_union = ds.sql("SELECT count(*) FROM all_city").fetchone()[0]
    dup = ds.sql(f"""
        SELECT count(*) FROM (
          SELECT medisoft_id, count(*) c FROM ({union_sql}) GROUP BY 1 HAVING count(*)>1
        )""").fetchone()[0]
    print(f"City-sheet rows (deduped by medisoft_id): {n_union}   (medisoft_ids appearing in >1 city: {dup})")
    print("  per city:")
    ds.sql("SELECT city, count(*) FROM all_city GROUP BY city ORDER BY city").show()

    # existing medisoft_ids from PG
    d = connect_to_postgres_via_duckdb()
    ex_csv = OUTPUT_DIR / "_existing_medisoft.csv"
    d.execute(f"COPY (SELECT DISTINCT medisoft_id FROM pg.bas_firms.cleaned_medisoft) TO '{_sql_literal(str(ex_csv))}' (HEADER)")
    ds.execute(f"CREATE TABLE existing AS SELECT medisoft_id FROM read_csv('{_sql_literal(str(ex_csv))}', header=true, all_varchar=true)")

    ds.execute("""
        CREATE TABLE new_rows AS
        SELECT a.* FROM all_city a
        LEFT JOIN existing e ON a.medisoft_id = e.medisoft_id
        WHERE e.medisoft_id IS NULL
    """)
    n_new = ds.sql("SELECT count(*) FROM new_rows").fetchone()[0]
    print(f"\nNEW medisoft rows (not in cleaned_medisoft): {n_new}   (already present: {n_union - n_new})")
    print("  new per city:")
    ds.sql("SELECT city, count(*) FROM new_rows GROUP BY city ORDER BY city").show()
    print("  sample:")
    ds.sql("SELECT medisoft_id, name, city, nb_patients, has_easybill_connection FROM new_rows ORDER BY city, medisoft_id LIMIT 20").show(max_width=160)

    cols = ", ".join(TARGET_COLS)
    out = OUTPUT_DIR / "cleaned_medisoft_new.csv"
    ds.execute(f"COPY (SELECT {cols} FROM new_rows) TO '{_sql_literal(str(out))}' (HEADER, DELIMITER ',')")
    ex_csv.unlink(missing_ok=True)
    print(f"\n✓ Wrote {out}")
    print("DONE (local only — no DB writes).")


if __name__ == "__main__":
    main()
