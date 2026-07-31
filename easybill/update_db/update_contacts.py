"""
Update easybill.contacts from an easybill "Contacts Export" CSV.

Steps:
  1. Load + clean the CSV (keep only Typ == 'Kunde', prefix columns "Kontakt: ").
  2. Make a full copy of the table BEFORE changing it (easybill.contacts_backup_<date>).
  3. Upsert on "Kontakt: Kundennummer": matched rows are DELETEd and re-INSERTed
     from the CSV (whole-row overwrite), unmatched CSV rows are inserted.

Run:  uv run python easybill/update_db/update_contacts.py
"""

import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from connection_alchemy import connect_to_db

CSV_PATH = "/Users/adrienblanquer/Downloads/Contacts-Export-02_07_2026-16_51_41.csv"
TABLE = "easybill.contacts"
BACKUP_TABLE = "easybill.contacts_backup_2026_07_02"
REPORT_PATH = str(Path(__file__).resolve().parent / "data" / "update_report_2026_07_02.txt")

# --- load + clean the CSV ----------------------------------------------------
df = pd.read_csv(CSV_PATH, sep=";", dtype=str, keep_default_na=False)
df = df[df["Typ"] == "Kunde"].copy()
if "Leere Spalte" in df.columns:  # present in some exports, absent in others
    df = df.drop(columns=["Leere Spalte"])
df.columns = [f"Kontakt: {c}" for c in df.columns]

INT_COLS = [
    "Kontakt: Kontakt ID",
    "Kontakt: Persönlich/Vertraulich",
    "Kontakt: Persönlich/Vertraulich-Lieferanschrift",
    "Kontakt: Skonto (Tage)",
    "Kontakt: Zahlungsziel (Tage)",
    "Kontakt: Lieferantennr. beim Kontakt",
    "Kontakt: Archiviert",
]
for c in INT_COLS:
    df[c] = pd.to_numeric(df[c].replace("", pd.NA), errors="coerce").astype("Int64")

for c in df.columns:
    if df[c].dtype == object:
        df[c] = df[c].where(df[c] != "", None)

assert df["Kontakt: Kundennummer"].is_unique, "Kundennummer must be unique among Kunde rows"
print(f"CSV rows ready to upsert: {len(df)}")

conn = connect_to_db()

# --- verify CSV columns match the DB table -----------------------------------
db_cols = [r[0] for r in conn.execute(text("""
    select column_name
    from information_schema.columns
    where table_schema = 'easybill' and table_name = 'contacts'
    order by ordinal_position
""")).fetchall()]

csv_only = [c for c in df.columns if c not in db_cols]
db_only = [c for c in db_cols if c not in df.columns]
print("CSV columns missing from DB:", csv_only)
print("DB columns missing from CSV (stay NULL on inserts):", db_only)
assert not csv_only, "CSV has columns the DB does not — refusing to proceed"

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
key = "Kontakt: Kundennummer"
csv_kundennr = df[key].tolist()

existing_total = conn.execute(text(f"select count(*) from {TABLE}")).scalar()
existing = pd.read_sql(
    text(f'select * from {TABLE} where "Kontakt: Kundennummer" = ANY(:knrs)'),
    conn,
    params={"knrs": csv_kundennr},
)
will_update = len(existing)
will_insert = len(df) - will_update
print(f"Existing rows in {TABLE}: {existing_total}")
print(f"  → matched (overwrite):  {will_update}")
print(f"  → unmatched (insert):   {will_insert}")
print(f"  → untouched in DB:      {existing_total - will_update}")

matched_knr = set(existing[key].astype(str)) if will_update else set()
inserted_knr = sorted(k for k in csv_kundennr if str(k) not in matched_knr)
updated_knr = sorted(str(k) for k in matched_knr)

# --- 2) upsert: delete matched, insert all CSV rows --------------------------
try:
    deleted = conn.execute(
        text(f'delete from {TABLE} where "Kontakt: Kundennummer" = ANY(:knrs)'),
        {"knrs": csv_kundennr},
    ).rowcount
    df.to_sql(
        "contacts",
        conn,
        schema="easybill",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )
    after = conn.execute(text(f"select count(*) from {TABLE}")).scalar()
    conn.commit()
    print(f"✓ deleted {deleted} rows, inserted {len(df)} rows")
    print(f"final row count in {TABLE}: {after}")
except Exception:
    conn.rollback()
    raise

# --- 3) write a persistent report --------------------------------------------
report = [
    "easybill.contacts update report",
    f"CSV source         : {CSV_PATH}",
    f"Backup table        : {BACKUP_TABLE}  ({n_backup} rows)",
    "",
    f"CSV Kunde rows      : {len(df)}",
    f"Rows before update  : {existing_total}",
    f"Rows after update   : {after}",
    f"Deleted (overwrite) : {deleted}",
    f"Updated (overwrite) : {will_update}",
    f"Inserted (new)      : {will_insert}",
    f"Untouched in DB     : {existing_total - will_update}",
    "",
    f"Inserted Kundennummer ({len(inserted_knr)}):",
    *[f"  {k}" for k in inserted_knr],
    "",
    f"Updated Kundennummer ({len(updated_knr)}):",
    *[f"  {k}" for k in updated_knr],
]
Path(REPORT_PATH).write_text("\n".join(report), encoding="utf-8")
print(f"✓ report written to {REPORT_PATH}")
