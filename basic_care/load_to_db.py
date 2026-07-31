import csv
import sys
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from connection_alchemy import connect_to_db

TABLE = "bas_firms.full_basic_care"
CSV_PATH = Path(__file__).parent / "full_basic_care_normalized.csv"


def main() -> None:
    with CSV_PATH.open(newline="") as f:
        reader = csv.DictReader(f)
        cols = reader.fieldnames
        rows = list(reader)

    quoted_cols = ", ".join(f'"{c}"' for c in cols)
    placeholders = ", ".join(f":{c}" for c in cols)
    insert_sql = f"insert into {TABLE} ({quoted_cols}) values ({placeholders})"

    conn = connect_to_db()
    trans = conn.begin()
    try:
        before = conn.execute(text(f"select count(*) from {TABLE}")).scalar()
        conn.execute(text(f"truncate table {TABLE}"))
        conn.execute(text(insert_sql), rows)
        after = conn.execute(text(f"select count(*) from {TABLE}")).scalar()
        trans.commit()
        print(f"replaced {before} rows with {after} rows in {TABLE}")
    except Exception:
        trans.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
