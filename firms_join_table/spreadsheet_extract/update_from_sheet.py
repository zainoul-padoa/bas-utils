"""
Update easybill_medisoft and cleaned_medisoft tables from Google Sheet.

Usage:
    python update_from_sheet.py --spreadsheet-id 1vB84YG3eBVJVAQsv8VNe2K_TTE8bZ1I9gnwidGAvyXE
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from datetime import datetime
import duckdb

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from merge_tables.db.connection import connect_to_postgres_via_duckdb
from config.mapping_spreadsheets import SPREADSHEET_MAPPING

DEFAULT_CREDENTIALS = SCRIPT_DIR / "config" / "gsheet-creds.json"
DEFAULT_SHEET_NAME = "easybill_clients_list"


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _sql_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def connect_gsheets(credentials_path: Path, database: str = ":memory:") -> duckdb.DuckDBPyConnection:
    credentials_path = credentials_path.expanduser().resolve()
    if not credentials_path.is_file():
        raise FileNotFoundError(f"Service account JSON not found: {credentials_path}")

    con = duckdb.connect(database)
    con.execute("INSTALL gsheets FROM community;")
    con.execute("LOAD gsheets;")
    con.execute(
        f"""
CREATE OR REPLACE SECRET gsheet_sa (
    TYPE gsheet,
    PROVIDER key_file,
    FILEPATH '{_sql_literal(str(credentials_path))}'
);
"""
    )
    return con


def backup_tables(duck, schema: str = "pg.bas_firms") -> tuple[str, str]:
    """Create timestamped backups of both tables. Returns backup table names."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    easybill_table = f"{schema}.easybill_medisoft"
    easybill_backup = f"{schema}.easybill_medisoft_backup_{timestamp}"

    cleaned_table = f"{schema}.cleaned_medisoft"
    cleaned_backup = f"{schema}.cleaned_medisoft_backup_{timestamp}"

    print(f"Creating backup: {easybill_backup}...")
    duck.execute(f"CREATE TABLE {easybill_backup} AS SELECT * FROM {easybill_table}")
    n1 = duck.sql(f"SELECT count(*) FROM {easybill_backup}").fetchone()[0]
    print(f"  ✓ Backed up {n1} rows")

    print(f"Creating backup: {cleaned_backup}...")
    duck.execute(f"CREATE TABLE {cleaned_backup} AS SELECT * FROM {cleaned_table}")
    n2 = duck.sql(f"SELECT count(*) FROM {cleaned_backup}").fetchone()[0]
    print(f"  ✓ Backed up {n2} rows")

    return easybill_backup, cleaned_backup


def read_spreadsheet_data(duck, spreadsheet_id: str, sheet_name: str) -> str:
    """Read data from Google Sheet and return as DuckDB table name."""
    table_name = "sheet_data"
    path_sql = _sql_literal(spreadsheet_id)
    sheet_sql = _sql_literal(sheet_name)

    print(f"Reading sheet '{sheet_name}' from spreadsheet {spreadsheet_id[:20]}...")

    # Try to load gsheets extension
    try:
        duck.execute("INSTALL gsheets FROM community;")
        duck.execute("LOAD gsheets;")
        print("  ✓ Loaded gsheets extension")
    except Exception as e:
        print(f"  WARNING: Could not load gsheets extension: {e}")
        print(f"  Please export the sheet to CSV and use --csv-path instead")
        raise

    duck.execute(
        f"""
        CREATE OR REPLACE TABLE {_sql_identifier(table_name)} AS
        SELECT *
        FROM read_gsheet(
            '{path_sql}',
            sheet='{sheet_sql}',
            all_varchar=true
        )
        """
    )

    n = duck.sql(f"SELECT count(*) FROM {_sql_identifier(table_name)}").fetchone()[0]
    print(f"  ✓ Loaded {n} rows from sheet")

    # Display schema
    schema = duck.sql(f"DESCRIBE {_sql_identifier(table_name)}")
    print("  Columns:")
    for row in schema.fetchall():
        print(f"    - {row[0]}: {row[1]}")

    return table_name


def upsert_cleaned_medisoft(duck_sheets, duck_pg, pg_prefix: str = "pg.bas_firms"):
    """
    UPSERT cleaned_medisoft by reading from all city sheets.

    City sheets: berlin, dusseldorf, frankfurt, hamburg, kiel, koln, munchen, rostock, stuttgart, viersen, other
    Skip: dashboard, easybill_clients_list
    """
    spreadsheet_id = "1vB84YG3eBVJVAQsv8VNe2K_TTE8bZ1I9gnwidGAvyXE"
    target_table = f"{pg_prefix}.cleaned_medisoft"

    city_sheets = [
        "berlin", "dusseldorf", "frankfurt", "hamburg", "kiel",
        "koln", "munchen", "rostock", "stuttgart", "viersen", "other"
    ]

    # Read and union all city sheets
    union_blocks = []
    for city_sheet in city_sheets:
        union_blocks.append(f"""
            SELECT
                medisoft_id,
                name,
                kuerzel,
                pfad,
                TRY_CAST(nb_patients AS INTEGER) AS nb_patients,
                last_exam_date,
                addresse AS address,
                CASE WHEN LOWER("has easybill connection") IN ('true', '1', 'yes') THEN TRUE
                     WHEN LOWER("has easybill connection") IN ('false', '0', 'no') THEN FALSE
                     ELSE NULL END AS has_easybill_connection,
                CASE WHEN LOWER("migrate as inactive") IN ('true', '1', 'yes') THEN TRUE
                     WHEN LOWER("migrate as inactive") IN ('false', '0', 'no') THEN FALSE
                     ELSE NULL END AS migrate_as_inactive,
                CASE WHEN LOWER("no migration") IN ('true', '1', 'yes') THEN TRUE
                     WHEN LOWER("no migration") IN ('false', '0', 'no') THEN FALSE
                     ELSE NULL END AS no_migration,
                CASE WHEN LOWER("Selbstzahler") IN ('true', '1', 'yes') THEN TRUE
                     WHEN LOWER("Selbstzahler") IN ('false', '0', 'no') THEN FALSE
                     ELSE NULL END AS selbstzahler,
                '{_sql_literal(city_sheet)}' AS city
            FROM read_gsheet(
                '{_sql_literal(spreadsheet_id)}',
                sheet='{_sql_literal(city_sheet)}',
                all_varchar=true
            )
        """)

    union_sql = " UNION ALL BY NAME ".join(union_blocks)

    # Create merged table
    merged_table = "medisoft_all_cities"
    duck_sheets.execute(f"""
        CREATE OR REPLACE TABLE {_sql_identifier(merged_table)} AS
        {union_sql}
    """)

    n_merged = duck_sheets.sql(f"SELECT count(*) FROM {_sql_identifier(merged_table)}").fetchone()[0]
    print(f"  ✓ Merged {n_merged} records from {len(city_sheets)} city sheets")

    # Export to CSV
    merged_csv = Path("/tmp/medisoft_all_cities.csv")
    duck_sheets.execute(f"COPY {_sql_identifier(merged_table)} TO '{_sql_literal(str(merged_csv))}' (HEADER, DELIMITER ',')")
    print(f"  ✓ Exported to {merged_csv}")

    # Load into PostgreSQL and UPSERT
    try:
        path_sql = _sql_literal(str(merged_csv))
        temp_table = f"{pg_prefix}.temp_medisoft"

        # Load CSV into temp table
        duck_pg.execute(f"""
            CREATE OR REPLACE TABLE {temp_table} AS
            SELECT * FROM read_csv('{path_sql}', header=true, all_varchar=true)
        """)

        n_temp = duck_pg.sql(f"SELECT count(*) FROM {temp_table}").fetchone()[0]
        print(f"  ✓ Loaded {n_temp} records into temporary table")

        # UPSERT with transaction (delete matching medisoft_ids, then insert)
        duck_pg.execute("BEGIN;")

        deleted = duck_pg.execute(
            f"""DELETE FROM {target_table}
               WHERE medisoft_id IN (SELECT DISTINCT medisoft_id FROM {temp_table})"""
        ).rowcount
        print(f"  ✓ Deleted {deleted} existing records")

        # Insert with proper type casting
        inserted = duck_pg.execute(f"""
            INSERT INTO {target_table}
            (medisoft_id, name, kuerzel, pfad, nb_patients, last_exam_date,
             address, has_easybill_connection, migrate_as_inactive, no_migration,
             selbstzahler, city)
            SELECT
                medisoft_id, name, kuerzel, pfad,
                COALESCE(TRY_CAST(nb_patients AS INTEGER), 0),
                last_exam_date, address,
                COALESCE(TRY_CAST(has_easybill_connection AS BOOLEAN), FALSE),
                COALESCE(TRY_CAST(migrate_as_inactive AS BOOLEAN), FALSE),
                COALESCE(TRY_CAST(no_migration AS BOOLEAN), FALSE),
                COALESCE(TRY_CAST(selbstzahler AS BOOLEAN), FALSE),
                city
            FROM {temp_table}
        """).rowcount
        print(f"  ✓ Inserted {inserted} new records")

        duck_pg.execute("COMMIT;")

        # Clean up
        duck_pg.execute(f"DROP TABLE IF EXISTS {temp_table}")

    except Exception as e:
        try:
            duck_pg.execute("ROLLBACK;")
        except:
            pass
        print(f"  ✗ ERROR: {e}")
        raise

    # Verify
    final_count = duck_pg.sql(f"SELECT count(*) FROM {target_table}").fetchone()[0]
    print(f"  ✓ Final row count in {target_table}: {final_count}")


def discover_table_schemas(duck, schema: str = "pg.bas_firms"):
    """Discover and display schemas of target tables."""
    print("\n" + "=" * 80)
    print("TARGET TABLE SCHEMAS")
    print("=" * 80)

    for table_name in ["easybill_medisoft", "cleaned_medisoft"]:
        full_name = f"{schema}.{table_name}"
        print(f"\n{full_name}:")
        schema_result = duck.sql(f"DESCRIBE {full_name}")
        for row in schema_result.fetchall():
            print(f"  {row[0]:<25} {row[1]}")

        count = duck.sql(f"SELECT count(*) FROM {full_name}").fetchone()[0]
        print(f"  (Current row count: {count})")


def upsert_easybill_medisoft(duck, source_table: str, target_schema: str = "pg.bas_firms"):
    """
    UPSERT easybill_medisoft table.

    Strategy:
    - Flatten multiline medisoft_ids into individual rows
    - Delete matching easybill_ids from target
    - Insert new data
    """
    source_id_col = None
    source_medisoft_col = None

    # Discover columns from source
    schema = duck.sql(f"DESCRIBE {_sql_identifier(source_table)}")
    columns = [row[0] for row in schema.fetchall()]
    print(f"\nSource table columns: {columns}")

    # Find the right columns
    for col in columns:
        if 'easybill' in col.lower() or 'kundennummer' in col.lower():
            source_id_col = col
        if 'medisoft' in col.lower() and 'id' in col.lower():
            source_medisoft_col = col

    if not source_id_col or not source_medisoft_col:
        print(f"  WARNING: Could not auto-detect columns. Found: {columns}")
        print(f"    Trying: {source_id_col} for easybill_id, {source_medisoft_col} for medisoft_id")

    target_table = f"{target_schema}.easybill_medisoft"

    print(f"\nUPSERTing {target_table}...")
    print(f"  Source columns: {source_id_col} → easybill_id, {source_medisoft_col} → medisoft_id")

    # Create flattened view with deduplicated medisoft_ids
    duck.execute(f"""
        CREATE OR REPLACE TABLE flattened_data AS
        WITH parsed AS (
            SELECT
                TRIM({_sql_identifier(source_id_col)}) AS easybill_id,
                TRIM(UNNEST(STRING_SPLIT(
                    COALESCE(NULLIF(TRIM({_sql_identifier(source_medisoft_col)}), ''), ''),
                    CHR(10)
                ))) AS medisoft_id
            FROM {_sql_identifier(source_table)}
        )
        SELECT DISTINCT easybill_id, NULLIF(medisoft_id, '') AS medisoft_id
        FROM parsed
        WHERE easybill_id <> ''
    """)

    n_flat = duck.sql("SELECT count(*) FROM flattened_data").fetchone()[0]
    print(f"  ✓ Flattened to {n_flat} unique pairs")

    # UPSERT with transaction
    duck.execute("BEGIN;")
    try:
        # Get list of easybill_ids being updated
        easybill_ids = duck.sql(
            "SELECT DISTINCT easybill_id FROM flattened_data"
        ).fetchall()
        easybill_ids_list = [f"'{_sql_literal(row[0])}'" for row in easybill_ids]

        if easybill_ids_list:
            ids_clause = ",".join(easybill_ids_list)
            # Delete existing entries for these ids
            deleted = duck.execute(
                f"DELETE FROM {target_table} WHERE easybill_id IN ({ids_clause})"
            ).rowcount
            print(f"  ✓ Deleted {deleted} existing rows")

            # Insert new data
            inserted = duck.execute(
                f"""
                INSERT INTO {target_table} (easybill_id, medisoft_id)
                SELECT easybill_id, medisoft_id FROM flattened_data
                """
            ).rowcount
            print(f"  ✓ Inserted {inserted} new rows")

        duck.execute("COMMIT;")
    except Exception as e:
        duck.execute("ROLLBACK;")
        print(f"  ✗ ERROR: {e}")
        raise

    # Verify
    final_count = duck.sql(f"SELECT count(*) FROM {target_table}").fetchone()[0]
    print(f"  ✓ Final row count in {target_table}: {final_count}")




def discover_table_schemas(duck, schema: str = "pg.bas_firms"):
    """Discover and display schemas of target tables."""
    print("\n" + "=" * 80)
    print("TARGET TABLE SCHEMAS")
    print("=" * 80)

    for table_name in ["easybill_medisoft", "cleaned_medisoft"]:
        full_name = f"{schema}.{table_name}"
        print(f"\n{full_name}:")
        schema_result = duck.sql(f"DESCRIBE {full_name}")
        for row in schema_result.fetchall():
            print(f"  {row[0]:<25} {row[1]}")

        count = duck.sql(f"SELECT count(*) FROM {full_name}").fetchone()[0]
        print(f"  (Current row count: {count})")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Update easybill_medisoft and cleaned_medisoft from Google Sheet"
    )
    parser.add_argument(
        "--spreadsheet-id",
        required=True,
        help="Google Sheet ID to read from",
    )
    parser.add_argument(
        "--sheet-name",
        default=DEFAULT_SHEET_NAME,
        help=f"Sheet name to read (default: {DEFAULT_SHEET_NAME})",
    )
    parser.add_argument(
        "--credentials",
        type=Path,
        default=DEFAULT_CREDENTIALS,
        help="Path to Google service account JSON",
    )
    parser.add_argument(
        "--pg-prefix",
        default="pg.bas_firms",
        help="Qualified schema prefix for PG tables",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done but don't make changes",
    )
    args = parser.parse_args()

    print("=" * 80)
    print("UPDATE DATABASE TABLES FROM GOOGLE SHEET")
    print("=" * 80)
    print(f"Spreadsheet ID: {args.spreadsheet_id}")
    print(f"Sheet name: {args.sheet_name}")
    print(f"Target schema: {args.pg_prefix}")
    if args.dry_run:
        print("MODE: DRY RUN (no changes)")
    print()

    # Create separate DuckDB connection for gsheets (before attaching PostgreSQL)
    print("Connecting to Google Sheet...")
    duck_sheets = connect_gsheets(args.credentials, database=":memory:")

    # Read spreadsheet data
    source_table = read_spreadsheet_data(duck_sheets, args.spreadsheet_id, args.sheet_name)

    # Now connect to PostgreSQL for database operations
    print("\nConnecting to PostgreSQL...")
    duck = connect_to_postgres_via_duckdb()

    # Discover table schemas
    print()
    discover_table_schemas(duck, args.pg_prefix)

    if args.dry_run:
        print("\n[DRY RUN] Would proceed with UPSERT. Exiting.")
        return

    # Create backups
    print()
    backup_tables(duck, args.pg_prefix)

    # UPSERT tables - need to copy data from sheets connection to postgres connection
    print("\nPreparing data for UPSERT...")

    # Export flattened pairs from sheets connection
    duck_sheets.execute(f"""
        CREATE OR REPLACE TABLE flattened_data AS
        WITH parsed AS (
            SELECT
                easybill_kundennummer AS easybill_id,
                TRIM(UNNEST(STRING_SPLIT(
                    COALESCE(NULLIF(TRIM(medisoft_ids), ''), ''),
                    CHR(10)
                ))) AS medisoft_id
            FROM {_sql_identifier(source_table)}
        )
        SELECT DISTINCT easybill_id, NULLIF(medisoft_id, '') AS medisoft_id
        FROM parsed
        WHERE easybill_id <> ''
    """)

    # Export to CSV for transfer
    flat_csv = Path("/tmp/flattened_pairs.csv")
    duck_sheets.execute(f"COPY flattened_data TO '{flat_csv}' (HEADER, DELIMITER ',')")
    print(f"  ✓ Exported flattened data to {flat_csv}")

    # Load into PostgreSQL and UPSERT
    print("\nUPSERTing easybill_medisoft...")
    target_table = f"{args.pg_prefix}.easybill_medisoft"

    try:
        # Load CSV into temp table (all in postgres database now)
        path_sql = _sql_literal(str(flat_csv))
        temp_table = f"{args.pg_prefix}.temp_updates"

        # Create temp table from CSV
        duck.execute(f"""
            CREATE OR REPLACE TABLE {temp_table} AS
            SELECT * FROM read_csv('{path_sql}', header=true, all_varchar=true)
        """)

        n_flat = duck.sql(f"SELECT count(*) FROM {temp_table}").fetchone()[0]
        print(f"  ✓ Loaded {n_flat} unique pairs")

        # UPSERT with transaction
        # Delete matching (easybill_id, medisoft_id) pairs, then insert all new pairs
        duck.execute("BEGIN;")

        # Delete existing entries matching BOTH easybill_id AND medisoft_id
        deleted = duck.execute(
            f"""DELETE FROM {target_table}
               WHERE (easybill_id, medisoft_id) IN
                     (SELECT DISTINCT easybill_id, medisoft_id FROM {temp_table})"""
        ).rowcount
        print(f"  ✓ Deleted {deleted} existing pairs")

        # Insert new data
        inserted = duck.execute(
            f"""INSERT INTO {target_table} (easybill_id, medisoft_id)
               SELECT easybill_id, medisoft_id FROM {temp_table}"""
        ).rowcount
        print(f"  ✓ Inserted {inserted} new pairs")

        duck.execute("COMMIT;")

        # Clean up temp table
        duck.execute(f"DROP TABLE IF EXISTS {temp_table}")

    except Exception as e:
        try:
            duck.execute("ROLLBACK;")
        except:
            pass
        print(f"  ✗ ERROR: {e}")
        raise

    # Verify
    final_count = duck.sql(f"SELECT count(*) FROM {target_table}").fetchone()[0]
    print(f"  ✓ Final row count in {target_table}: {final_count}")

    # UPSERT cleaned_medisoft from multiple city sheets
    print("\nUPSERTing cleaned_medisoft from city sheets...")
    upsert_cleaned_medisoft(duck_sheets, duck, args.pg_prefix)

    print("\n" + "=" * 80)
    print("✓ UPDATE COMPLETE!")
    print("=" * 80)


if __name__ == "__main__":
    main()
