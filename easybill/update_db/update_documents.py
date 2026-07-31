"""
Update easybill.documents from easybill "Documents Export" CSV(s).

The export grain is one row per document line-item ("Posten"), so a single
"Dokument: ID" spans several rows. The upsert therefore works at the DOCUMENT
level (whole-document overwrite): every row whose "Dokument: ID" appears in the
CSV is DELETEd, then all CSV rows are re-INSERTed. Documents absent from the CSV
are left untouched.

Steps:
  1. Load + concat the CSV(s) (columns already carry "Dokument: "/"Kontakt: "/
     "Posten: " prefixes, so no renaming). Coerce the DB's numeric columns.
  2. Full copy of the table BEFORE changing it (easybill.documents_backup_<date>).
  3. Upsert on "Dokument: ID", then write a persistent report.

Run:  uv run python easybill/update_db/update_documents.py
"""

import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from connection_alchemy import connect_to_db

CSV_PATHS = [
    "/Users/adrienblanquer/Downloads/Documents-Export-02_07_2026-16_57_24 2.csv",
    "/Users/adrienblanquer/Downloads/Documents-Export-02_07_2026-16_57_34.csv",
]
TABLE = "easybill.documents"
BACKUP_TABLE = "easybill.documents_backup_2026_07_02"
REPORT_PATH = str(Path(__file__).resolve().parent / "data" / "documents_update_report_2026_07_02.txt")
KEY = "Dokument: ID"

# --- load + concat the CSV(s) ------------------------------------------------
frames = []
for p in CSV_PATHS:
    d = pd.read_csv(p, sep=";", dtype=str, keep_default_na=False)
    print(f"loaded {len(d)} rows from {Path(p).name}")
    frames.append(d)
df = pd.concat(frames, ignore_index=True)
print(f"combined rows: {len(df)} | distinct {KEY}: {df[KEY].nunique()}")

conn = connect_to_db()

# --- verify CSV columns match the DB table, learn column types ---------------
schema = conn.execute(text("""
    select column_name, data_type
    from information_schema.columns
    where table_schema = 'easybill' and table_name = 'documents'
    order by ordinal_position
""")).fetchall()
db_cols = [r[0] for r in schema]
db_types = {r[0]: r[1] for r in schema}

csv_only = [c for c in df.columns if c not in db_cols]
db_only = [c for c in db_cols if c not in df.columns]
print("CSV columns missing from DB:", csv_only)
print("DB columns missing from CSV (stay NULL on inserts):", db_only)
assert not csv_only, "CSV has columns the DB does not — refusing to proceed"

# integer-like DB columns that exist in the CSV -> nullable Int64
INT_TYPES = {"smallint", "integer", "bigint"}
int_cols = [c for c in df.columns if db_types.get(c) in INT_TYPES]
for c in int_cols:
    df[c] = pd.to_numeric(df[c].replace("", pd.NA), errors="coerce").astype("Int64")
print(f"coerced {len(int_cols)} numeric columns")

# empty strings in text columns -> NULL
for c in df.columns:
    if df[c].dtype == object:
        df[c] = df[c].where(df[c] != "", None)

# --- 1) backup the table BEFORE any change -----------------------------------
exists = conn.execute(text("""
    select 1 from information_schema.tables
    where table_schema = 'easybill' and table_name = :t
"""), {"t": BACKUP_TABLE.split(".", 1)[1]}).scalar()
if exists:
    raise SystemExit(f"Backup table {BACKUP_TABLE} already exists — aborting to avoid overwrite.")

try:
    conn.execute(text(f"create table {BACKUP_TABLE} as select * from {TABLE}"))
    n_backup = conn.execute(text(f"select count(*) from {BACKUP_TABLE}")).scalar()
    conn.commit()
    print(f"✓ backed up {n_backup} rows to {BACKUP_TABLE}")
except Exception:
    conn.rollback()
    raise

# --- summary of what the upsert will do --------------------------------------
# KEY is bigint in the DB -> pass Python ints
csv_doc_ids = [int(x) for x in df[KEY].dropna().unique().tolist()]

rows_before = conn.execute(text(f"select count(*) from {TABLE}")).scalar()
docs_before = conn.execute(text(f'select count(distinct "{KEY}") from {TABLE}')).scalar()
docs_overwritten = conn.execute(
    text(f'select count(distinct "{KEY}") from {TABLE} where "{KEY}" = ANY(:ids)'),
    {"ids": csv_doc_ids},
).scalar()
docs_new = len(csv_doc_ids) - docs_overwritten
print(f"Rows before: {rows_before} | documents before: {docs_before}")
print(f"  → documents overwritten: {docs_overwritten}")
print(f"  → documents new:         {docs_new}")
print(f"  → documents untouched:   {docs_before - docs_overwritten}")

# --- 2) upsert: delete matched documents, COPY all CSV rows ------------------
# COPY is used instead of df.to_sql(method='multi'): with 232 columns the
# multi-row INSERT compilation is pathologically slow (minutes, CPU-bound),
# whereas COPY streams the whole batch in seconds.
import csv
import io

col_list = ", ".join('"' + c.replace('"', '""') + '"' for c in df.columns)
copy_sql = f"COPY {TABLE} ({col_list}) FROM STDIN WITH (FORMAT csv, NULL '')"

buf = io.StringIO()
df.to_csv(buf, index=False, header=False, quoting=csv.QUOTE_MINIMAL, na_rep="")
buf.seek(0)

try:
    deleted = conn.execute(
        text(f'delete from {TABLE} where "{KEY}" = ANY(:ids)'),
        {"ids": csv_doc_ids},
    ).rowcount
    raw = conn.connection  # DBAPI (psycopg2) connection from the SQLAlchemy conn
    cur = raw.cursor()
    cur.copy_expert(copy_sql, buf)
    cur.close()
    rows_after = conn.execute(text(f"select count(*) from {TABLE}")).scalar()
    docs_after = conn.execute(text(f'select count(distinct "{KEY}") from {TABLE}')).scalar()
    conn.commit()
    print(f"✓ deleted {deleted} rows, inserted {len(df)} rows")
    print(f"final: {rows_after} rows | {docs_after} documents")
except Exception:
    conn.rollback()
    raise

# --- 3) write a persistent report --------------------------------------------
report = [
    "easybill.documents update report",
    f"CSV sources         : {', '.join(Path(p).name for p in CSV_PATHS)}",
    f"Backup table        : {BACKUP_TABLE}  ({n_backup} rows)",
    "",
    f"CSV rows (line-items): {len(df)}",
    f"CSV documents        : {len(csv_doc_ids)}",
    "",
    f"Rows before update   : {rows_before}",
    f"Rows after update    : {rows_after}",
    f"Deleted (overwrite)  : {deleted}",
    f"Inserted             : {len(df)}",
    "",
    f"Documents before     : {docs_before}",
    f"Documents after      : {docs_after}",
    f"Documents overwritten: {docs_overwritten}",
    f"Documents new        : {docs_new}",
    f"Documents untouched  : {docs_before - docs_overwritten}",
    "",
    f"DB-only columns left NULL on inserts: {db_only}",
]
Path(REPORT_PATH).write_text("\n".join(report), encoding="utf-8")
print(f"✓ report written to {REPORT_PATH}")
