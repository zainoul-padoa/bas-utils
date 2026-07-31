"""Load extracted CSVs into PostgreSQL via DuckDB (attached as pg)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from merge_tables.db.connection import connect_to_postgres_via_duckdb

DEFAULT_MEDISOFT_CSV = SCRIPT_DIR / "output" / "easybill_medisoft_pairs.csv"
DEFAULT_ZOHO_CSV = SCRIPT_DIR / "output" / "berlin_clientlist_zoho.csv"
DEFAULT_PG_PREFIX = "pg.bas_firms"


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _path_or_skip(value: str) -> Path | None:
    if value == "":
        return None
    return Path(value)


def _load_medisoft(duck, pg_table: str, csv_path: Path) -> None:
    path_sql = _sql_literal(str(csv_path.expanduser().resolve()))
    duck.execute("BEGIN;")
    try:
        duck.execute(f"TRUNCATE TABLE {pg_table};")
        duck.execute(
            f"""
            INSERT INTO {pg_table} (easybill_id, medisoft_id)
            SELECT
                easybill_kundennummer,
                NULLIF(TRIM(medisoft_id), '')
            FROM read_csv(
                '{path_sql}',
                header = true,
                all_varchar = true
            );
            """
        )
        duck.execute("COMMIT;")
    except Exception:
        duck.execute("ROLLBACK;")
        raise


def _load_zoho(duck, pg_table: str, csv_path: Path) -> None:
    path_sql = _sql_literal(str(csv_path.expanduser().resolve()))
    duck.execute("BEGIN;")
    try:
        duck.execute(f"TRUNCATE TABLE {pg_table};")
        duck.execute(
            f"""
            INSERT INTO {pg_table} (easybill_id, zoho_id)
            SELECT
                easybill_kundennummer,
                NULLIF(TRIM(zoho_id), '')
            FROM read_csv(
                '{path_sql}',
                header = true,
                all_varchar = true
            );
            """
        )
        duck.execute("COMMIT;")
    except Exception:
        duck.execute("ROLLBACK;")
        raise


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Truncate and load easybill_medisoft / easybill_zoho from CSV into PostgreSQL.",
    )
    parser.add_argument(
        "--medisoft-csv",
        type=_path_or_skip,
        default=DEFAULT_MEDISOFT_CSV,
        help="easybill_kundennummer, medisoft_id CSV (empty string to skip)",
    )
    parser.add_argument(
        "--zoho-csv",
        type=_path_or_skip,
        default=DEFAULT_ZOHO_CSV,
        help="easybill_kundennummer, zoho_id CSV (empty string to skip)",
    )
    parser.add_argument(
        "--pg-prefix",
        default=DEFAULT_PG_PREFIX,
        help="Qualified schema prefix for PG tables (default: pg.bas_firms)",
    )
    args = parser.parse_args()

    medisoft_table = f"{args.pg_prefix}.easybill_medisoft"
    zoho_table = f"{args.pg_prefix}.easybill_zoho"

    if args.medisoft_csv is None and args.zoho_csv is None:
        parser.error("Provide at least one of --medisoft-csv or --zoho-csv (non-empty).")

    duck = connect_to_postgres_via_duckdb()

    if args.medisoft_csv is not None:
        if not args.medisoft_csv.is_file():
            raise FileNotFoundError(f"Medisoft CSV not found: {args.medisoft_csv}")
        _load_medisoft(duck, medisoft_table, args.medisoft_csv)
        n = duck.sql(f"SELECT count(*) FROM {medisoft_table}").fetchone()[0]
        print(f"Loaded {n} rows into {medisoft_table}.")

    if args.zoho_csv is not None:
        if not args.zoho_csv.is_file():
            raise FileNotFoundError(f"Zoho CSV not found: {args.zoho_csv}")
        _load_zoho(duck, zoho_table, args.zoho_csv)
        n = duck.sql(f"SELECT count(*) FROM {zoho_table}").fetchone()[0]
        print(f"Loaded {n} rows into {zoho_table}.")


if __name__ == "__main__":
    main()
