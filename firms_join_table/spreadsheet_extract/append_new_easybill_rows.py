"""
Append new easybill client rows to the `easybill_clients_list` sheet.

Reads ./output/easybill_new_rows.csv (produced by prep_new_easybill_rows.py)
and appends them after the last data row via gspread, using RAW input so
values are stored verbatim as text.

Safety:
  - Verifies the sheet header still matches the expected column order.
  - Re-checks against the live sheet's Kundennummern so it never appends a
    client that already exists (idempotent).

Usage:
    uv run python append_new_easybill_rows.py            # dry run (default)
    uv run python append_new_easybill_rows.py --apply     # actually append
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

import gspread

SCRIPT_DIR = Path(__file__).resolve().parent
SPREADSHEET_ID = "1vB84YG3eBVJVAQsv8VNe2K_TTE8bZ1I9gnwidGAvyXE"
SHEET_NAME = "easybill_clients_list"
CREDS = SCRIPT_DIR / "config" / "gsheet-creds.json"
NEW_CSV = SCRIPT_DIR / "output" / "easybill_new_rows.csv"

SHEET_COLUMNS = [
    "easybill_kundennummer", "last_invoice_date", "€ net billed", "spe. care",
    "wochenliste_ids", "easybill_firma", "easybill_name", "easybill_vorname",
    "easybill_address", "medisoft_ids", "medisoft_names", "sim_scores",
    "zoho_id", "link to zoho", "validated", "no_migration",
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Actually append (default dry-run)")
    args = parser.parse_args()

    if not NEW_CSV.is_file():
        raise FileNotFoundError(f"Missing {NEW_CSV}. Run prep_new_easybill_rows.py first.")

    with NEW_CSV.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != SHEET_COLUMNS:
            raise SystemExit(f"CSV columns don't match expected order:\n{reader.fieldnames}")
        csv_rows = list(reader)

    gc = gspread.service_account(filename=str(CREDS))
    ws = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)

    header = ws.row_values(1)
    if header != SHEET_COLUMNS:
        raise SystemExit(f"Sheet header changed, aborting.\nexpected: {SHEET_COLUMNS}\ngot:      {header}")

    existing_kn = {k.strip() for k in ws.col_values(1)[1:] if k.strip()}
    to_append = [r for r in csv_rows if r["easybill_kundennummer"].strip() not in existing_kn]
    skipped = len(csv_rows) - len(to_append)

    values = [[r[c] for c in SHEET_COLUMNS] for r in to_append]

    print("=" * 70)
    print("APPEND new rows to", SHEET_NAME)
    print("=" * 70)
    print(f"  Existing data rows:      {len(existing_kn)}")
    print(f"  Rows in CSV:             {len(csv_rows)}")
    print(f"  Already present (skip):  {skipped}")
    print(f"  To append:               {len(values)}")
    if values:
        print("\n  First row:", values[0])
        print("  Last row: ", values[-1])

    if not args.apply:
        print("\n[DRY RUN] Nothing written. Re-run with --apply.")
        return
    if not values:
        print("\nNothing to append.")
        return

    ws.append_rows(values, value_input_option="RAW", insert_data_option="INSERT_ROWS")
    after = len([k for k in ws.col_values(1)[1:] if k.strip()])
    print(f"\n  ✓ Appended {len(values)} rows. Sheet now has {after} data rows.")
    print("✓ DONE.")


if __name__ == "__main__":
    main()
