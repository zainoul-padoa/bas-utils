"""
Push easybill/output/all_clients_rebuilt.csv into the "All clients" spreadsheet
as a NEW tab (non-destructive) for review before swapping.

Run from the repo root:  uv run python billing/write_all_clients_to_sheet.py
"""

from pathlib import Path

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

ROOT = Path(__file__).resolve().parent.parent
SPREADSHEET_ID = "1Rz_dGw4y1b1ym1QN-XFV9W5FtfZShmVQKeq0bRQgnQM"
NEW_TAB = "All clients (rebuilt)"
SRC = ROOT / "easybill" / "output" / "all_clients_rebuilt.csv"
CREDS = ROOT / "billing" / "config" / "gsheet-creds.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def main() -> None:
    df = pd.read_csv(SRC, dtype=str).fillna("")
    creds = Credentials.from_service_account_file(str(CREDS), scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)

    # replace the review tab if a previous run left one behind
    for ws in sh.worksheets():
        if ws.title == NEW_TAB:
            sh.del_worksheet(ws)
            break

    ws = sh.add_worksheet(title=NEW_TAB, rows=len(df) + 10, cols=len(df.columns) + 2)
    ws.update([df.columns.tolist()] + df.values.tolist(), value_input_option="RAW")

    print(f"✓ wrote {len(df)} rows to tab '{NEW_TAB}'")
    print(f"  https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit#gid={ws.id}")


if __name__ == "__main__":
    main()
