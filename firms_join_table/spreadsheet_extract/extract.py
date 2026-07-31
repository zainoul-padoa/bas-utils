"""Load city spreadsheets from SPREADSHEET_MAPPING into DuckDB and build merged easybill↔medisoft pairs."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
import duckdb
from config.mapping_spreadsheets import SPREADSHEET_MAPPING


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

DEFAULT_CREDENTIALS = SCRIPT_DIR / "config" / "gsheet-creds.json"
DEFAULT_OUTPUT_CSV = SCRIPT_DIR / "output" / "easybill_medisoft_pairs.csv"
DEFAULT_BERLIN_ZOHO_CSV = SCRIPT_DIR / "output" / "berlin_clientlist_zoho.csv"
DEFAULT_MEDISOFT_OUTPUT_CSV = SCRIPT_DIR / "output" / "medisoft_union.csv"
DEFAULT_CACHE_DIR = SCRIPT_DIR / "output" / "cache_tables"

MERGED_PAIRS_SQL = """
WITH merged_couples AS (
{union_body}
),
distinct_couples AS (
    SELECT DISTINCT
        easybill_kundennummer,
        last_invoice_date,
        "€ net billed",
        medisoft_id
    FROM merged_couples
)
SELECT *
FROM distinct_couples d
WHERE d.medisoft_id IS NOT NULL
   OR NOT EXISTS (
       SELECT 1
       FROM distinct_couples d2
       WHERE d2.easybill_kundennummer = d.easybill_kundennummer
         AND d2.medisoft_id IS NOT NULL
   )
ORDER BY d.easybill_kundennummer
"""


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _sql_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _path_or_skip(value: str) -> Path | None:
    """Empty string disables optional CSV output (Path('') would wrongly resolve to '.')."""
    if value == "":
        return None
    return Path(value)


def _city_cache_path(cache_dir: Path, city: str) -> Path:
    slug = "".join(ch.lower() if ch.isalnum() else "_" for ch in city).strip("_")
    return cache_dir / f"{slug}.parquet"


def _city_cache_path_with_suffix(cache_dir: Path, city: str, suffix: str) -> Path:
    slug = "".join(ch.lower() if ch.isalnum() else "_" for ch in city).strip("_")
    return cache_dir / f"{slug}_{suffix}.parquet"


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


def load_city_tables(
    con: duckdb.DuckDBPyConnection,
    cache_dir: Path,
    update_cache: bool = False,
) -> None:
    cache_dir = cache_dir.expanduser().resolve()
    cache_dir.mkdir(parents=True, exist_ok=True)
    for source in SPREADSHEET_MAPPING.sheets:
        table_name = _sql_identifier(source.city)
        cache_path = _city_cache_path(cache_dir, source.city)
        if cache_path.is_file() and not update_cache:
            con.execute(
                f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT *
                FROM read_parquet('{_sql_literal(str(cache_path))}');
                """
            )
            n = con.sql(f"SELECT count(*) AS n FROM {table_name}").fetchone()[0]
            print(f"Loaded table {table_name} from cache with {n} rows.")
            continue

        spreadsheet_id = _sql_literal(source.spreadsheet_id)
        sheet_name = _sql_literal(source.easybill_spreadsheet_name)
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT *
            FROM read_gsheet(
                '{spreadsheet_id}',
                sheet='{sheet_name}',
                all_varchar=true
            );"""
        )
        con.execute(
            f"COPY {table_name} TO '{_sql_literal(str(cache_path))}' (FORMAT PARQUET);"
        )
        n = con.sql(f"SELECT count(*) AS n FROM {table_name}").fetchone()[0]
        print(f"Loaded table {table_name} from gsheet with {n} rows. Updated cache at {cache_path}.")


def load_medisoft_tables(
    con: duckdb.DuckDBPyConnection,
    cache_dir: Path,
    update_cache: bool = False,
) -> None:
    cache_dir = cache_dir.expanduser().resolve()
    cache_dir.mkdir(parents=True, exist_ok=True)
    for source in SPREADSHEET_MAPPING.sheets:
        table_name = _sql_identifier(f"{source.city}__medisoft")
        cache_path = _city_cache_path_with_suffix(cache_dir, source.city, "medisoft")
        if cache_path.is_file() and not update_cache:
            con.execute(
                f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT *
                FROM read_parquet('{_sql_literal(str(cache_path))}');
                """
            )
            n = con.sql(f"SELECT count(*) AS n FROM {table_name}").fetchone()[0]
            print(f"Loaded medisoft table {table_name} from cache with {n} rows.")
            continue

        spreadsheet_id = _sql_literal(source.spreadsheet_id)
        sheet_name = _sql_literal(source.medisoft_spreadsheet_name)
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT *
            FROM read_gsheet(
                '{spreadsheet_id}',
                sheet='{sheet_name}',
                all_varchar=true
            );"""
        )
        con.execute(
            f"COPY {table_name} TO '{_sql_literal(str(cache_path))}' (FORMAT PARQUET);"
        )
        n = con.sql(f"SELECT count(*) AS n FROM {table_name}").fetchone()[0]
        print(f"Loaded medisoft table {table_name} from gsheet with {n} rows. Updated cache at {cache_path}.")


def merged_couples_union_sql() -> str:
    newline = chr(10)
    blocks: list[str] = []
    for source in SPREADSHEET_MAPPING.sheets:
        city_lit = _sql_literal(source.city)
        table = _sql_identifier(source.city)
        blocks.append(
            f"""
        SELECT
            easybill_kundennummer,
            last_invoice_date,
            "€ net billed",
            unnest(COALESCE(string_split(medisoft_ids, '{newline}'), [NULL])) AS medisoft_id,
            '{city_lit}' AS city,
        FROM {table}
        """
        )
    return " UNION ALL ".join(blocks)


def merged_medisoft_pairs_sql() -> str:
    return MERGED_PAIRS_SQL.format(union_body=merged_couples_union_sql())


def medisoft_union_sql() -> str:
    blocks: list[str] = []
    for source in SPREADSHEET_MAPPING.sheets:
        city_lit = _sql_literal(source.city)
        table = _sql_identifier(f"{source.city}__medisoft")
        blocks.append(
            f"""
        SELECT
            *,
            '{city_lit}' AS city
        FROM {table}
        """
        )
    return " UNION ALL BY NAME ".join(blocks)


def create_medisoft_union_table(con: duckdb.DuckDBPyConnection, table_name: str = "medisoft_union") -> None:
    con.execute(
        f"CREATE OR REPLACE TABLE {_sql_identifier(table_name)} AS {medisoft_union_sql()}"
    )


def create_merged_pairs_table(con: duckdb.DuckDBPyConnection, table_name: str = "easybill_medisoft_pairs") -> None:
    con.execute(
        f"CREATE OR REPLACE TABLE {_sql_identifier(table_name)} AS {merged_medisoft_pairs_sql()}"
    )


def export_table_csv(con: duckdb.DuckDBPyConnection, table_name: str, csv_path: Path) -> None:
    csv_path = csv_path.expanduser().resolve()
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    table_sql = _sql_identifier(table_name)
    path_sql = _sql_literal(str(csv_path))
    con.execute(
        f"COPY {table_sql} TO '{path_sql}' (HEADER, DELIMITER ',');",
    )


def export_select_csv(con: duckdb.DuckDBPyConnection, select_sql: str, csv_path: Path) -> None:
    csv_path = csv_path.expanduser().resolve()
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    path_sql = _sql_literal(str(csv_path))
    con.execute(f"COPY ({select_sql}) TO '{path_sql}' (HEADER, DELIMITER ',');")


def berlin_clientlist_zoho_select_sql() -> str:
    return (
        f"SELECT easybill_kundennummer, zoho_id FROM {_sql_identifier('Berlin')} ORDER BY easybill_kundennummer"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract city spreadsheets into DuckDB and merge medisoft ids.")
    parser.add_argument(
        "--credentials",
        type=Path,
        default=DEFAULT_CREDENTIALS,
        help="Path to Google service account JSON",
    )
    parser.add_argument(
        "--db",
        default=":memory:",
        help="DuckDB path or :memory: (default)",
    )
    parser.add_argument(
        "--merged-table",
        default="easybill_medisoft_pairs",
        help="Table name for deduped easybill_kundennummer / medisoft_id pairs (empty to skip)",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=DEFAULT_OUTPUT_CSV,
        help="Merged pairs CSV path",
    )
    parser.add_argument(
        "--berlin-zoho-csv",
        type=_path_or_skip,
        default=DEFAULT_BERLIN_ZOHO_CSV,
        help=(
            "Berlin clientlist: write easybill_kundennummer + zoho_id to this path "
            "(pass empty string to skip)"
        ),
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=DEFAULT_CACHE_DIR,
        help="Directory for cached city table parquet files",
    )
    parser.add_argument(
        "--update-cache",
        action="store_true",
        help="Refresh local cache from Google Sheets before extracting",
    )
    parser.add_argument(
        "--medisoft-table",
        default="medisoft_union",
        help="Table name for unioned medisoft spreadsheets (empty to skip)",
    )
    parser.add_argument(
        "--medisoft-csv",
        type=Path,
        default=DEFAULT_MEDISOFT_OUTPUT_CSV,
        help="Unioned medisoft CSV path",
    )
    args = parser.parse_args()

    con = connect_gsheets(args.credentials, database=args.db)
    load_city_tables(con, cache_dir=args.cache_dir, update_cache=args.update_cache)
    load_medisoft_tables(con, cache_dir=args.cache_dir, update_cache=args.update_cache)
    print(con.sql("SHOW TABLES").df().to_string(index=False))

    if args.berlin_zoho_csv:
        export_select_csv(con, berlin_clientlist_zoho_select_sql(), args.berlin_zoho_csv)
        print(f"Wrote Berlin clientlist (zoho) {args.berlin_zoho_csv.expanduser().resolve()}")

    if args.merged_table:
        create_merged_pairs_table(con, args.merged_table)
        n = con.sql(f"SELECT count(*) AS n FROM {_sql_identifier(args.merged_table)}").fetchone()[0]
        print(f"Created table {args.merged_table!r} with {n} rows.")
        export_table_csv(con, args.merged_table, args.output_csv)
        print(f"Wrote {args.output_csv.expanduser().resolve()}")

    if args.medisoft_table:
        create_medisoft_union_table(con, args.medisoft_table)
        n = con.sql(f"SELECT count(*) AS n FROM {_sql_identifier(args.medisoft_table)}").fetchone()[0]
        print(f"Created table {args.medisoft_table!r} with {n} rows.")
        export_table_csv(con, args.medisoft_table, args.medisoft_csv)
        print(f"Wrote {args.medisoft_csv.expanduser().resolve()}")


if __name__ == "__main__":
    main()
