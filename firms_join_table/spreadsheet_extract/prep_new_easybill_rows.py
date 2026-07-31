"""
LOCAL-ONLY: build the new rows to append to the `easybill_clients_list` sheet.

Source of truth: pg.easybill.contacts. A contact is "new" when its
Kundennummer is not already a row in the sheet. Only the easybill identity
columns are filled (kundennummer, firma, name, vorname, address); all
enrichment columns (medisoft_ids, zoho_id, ...) are left empty.

Archived contacts (Kontakt: Archiviert = 1) are excluded.

Writes ./output/easybill_new_rows.csv in the sheet's exact column order.
Does NOT write to the sheet.
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
SHEET_NAME = "easybill_clients_list"
OUTPUT_DIR = SCRIPT_DIR / "output"

# Exact sheet column order.
SHEET_COLUMNS = [
    "easybill_kundennummer", "last_invoice_date", "€ net billed", "spe. care",
    "wochenliste_ids", "easybill_firma", "easybill_name", "easybill_vorname",
    "easybill_address", "medisoft_ids", "medisoft_names", "sim_scores",
    "zoho_id", "link to zoho", "validated", "no_migration",
]


def main() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)

    # 1. existing sheet Kundennummern (LOCAL)
    ds = connect_gsheets(DEFAULT_CREDENTIALS)
    ds.execute(
        f"""CREATE TABLE sheet AS SELECT * FROM read_gsheet(
            '{_sql_literal(SPREADSHEET_ID)}', sheet='{_sql_literal(SHEET_NAME)}', all_varchar=true)"""
    )
    kn_csv = OUTPUT_DIR / "_sheet_kn.csv"
    ds.execute(
        f"""COPY (SELECT DISTINCT TRIM(easybill_kundennummer) AS kn FROM sheet
                  WHERE NULLIF(TRIM(easybill_kundennummer),'') IS NOT NULL)
            TO '{_sql_literal(str(kn_csv))}' (HEADER)"""
    )

    # 2. new contacts from PG, mapped to sheet columns
    d = connect_to_postgres_via_duckdb()
    d.execute(
        f"""CREATE TABLE sheet_kn AS
            SELECT kn FROM read_csv('{_sql_literal(str(kn_csv))}', header=true, all_varchar=true)"""
    )
    d.execute(
        """
        CREATE TABLE new_rows AS
        SELECT
            TRIM("Kontakt: Kundennummer")                       AS easybill_kundennummer,
            ''                                                  AS last_invoice_date,
            ''                                                  AS "€ net billed",
            ''                                                  AS "spe. care",
            ''                                                  AS wochenliste_ids,
            COALESCE("Kontakt: Firma", '')                      AS easybill_firma,
            COALESCE("Kontakt: Name", '')                       AS easybill_name,
            COALESCE("Kontakt: Vorname", '')                    AS easybill_vorname,
            TRIM(regexp_replace(
                concat_ws(' ',
                    NULLIF(TRIM("Kontakt: Straße/Hausnummer"), ''),
                    NULLIF(TRIM("Kontakt: Postleitzahl"), ''),
                    NULLIF(TRIM("Kontakt: Ort"), '')
                ), '\\s+', ' ', 'g'))                            AS easybill_address,
            ''                                                  AS medisoft_ids,
            ''                                                  AS medisoft_names,
            ''                                                  AS sim_scores,
            ''                                                  AS zoho_id,
            ''                                                  AS "link to zoho",
            ''                                                  AS validated,
            ''                                                  AS no_migration
        FROM pg.easybill.contacts
        WHERE NULLIF(TRIM("Kontakt: Kundennummer"), '') IS NOT NULL
          AND TRIM("Kontakt: Kundennummer") NOT IN (SELECT kn FROM sheet_kn)
          AND COALESCE("Kontakt: Archiviert", 0) <> 1
        ORDER BY easybill_kundennummer
        """
    )
    n = d.sql("SELECT count(*) FROM new_rows").fetchone()[0]
    print(f"New rows to append: {n}")
    print("\nSample:")
    d.sql(
        "SELECT easybill_kundennummer, easybill_firma, easybill_vorname, easybill_name, easybill_address FROM new_rows LIMIT 15"
    ).show(max_width=160)

    out = OUTPUT_DIR / "easybill_new_rows.csv"
    # ensure column order
    cols = ", ".join(f'"{c}"' for c in SHEET_COLUMNS)
    d.execute(f"COPY (SELECT {cols} FROM new_rows) TO '{_sql_literal(str(out))}' (HEADER, DELIMITER ',')")
    kn_csv.unlink(missing_ok=True)
    print(f"\n✓ Wrote {out}")
    print("DONE (local only — nothing written to the sheet).")


if __name__ == "__main__":
    main()
