
import time
import requests
import json
import os

from dotenv import load_dotenv

# ==============
# CONFIG
# ==============
ZOHO_DOMAIN = "https://www.zohoapis.eu"
CLIENT_ID = '1000.UM9NBC0TBJ49IQXT7M6YLDP3OOL5AB'
load_dotenv()

CLIENT_SECRET = os.getenv('CLIENT_SECRET_ZOHO')
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN_ZOHO")

ACCESS_TOKEN = None

# ==============
# AUTH
# ==============
def refresh_access_token():
    global ACCESS_TOKEN
    url = "https://accounts.zoho.eu/oauth/v2/token"
    params = {
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token"
    }
    res = requests.post(url, params=params)
    res.raise_for_status()
    data = res.json()
    ACCESS_TOKEN = data["access_token"]
    print("Access token refreshed")
    return ACCESS_TOKEN

def zoho_headers():
    if not ACCESS_TOKEN:
        refresh_access_token()
    return {
        "Authorization": f"Zoho-oauthtoken {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

def request_with_refresh(method, url, **kwargs):
    """Wrapper around requests to auto-refresh token on 401 errors."""
    res = requests.request(method, url, headers=zoho_headers(), **kwargs)
    if res.status_code == 401:
        print("Access token expired, refreshing...")
        refresh_access_token()
        res = requests.request(method, url, headers=zoho_headers(), **kwargs)
    res.raise_for_status()
    return res

# ==============
# FETCH ALL RECORD IDS
# ==============
def get_all_record_ids(module: str) -> list[str]:
    """Paginate through all records in a module and return every ID."""
    ids = []
    page = 1
    per_page = 200

    while True:
        url = f"{ZOHO_DOMAIN}/crm/v6/{module}"
        params = {
            "fields": "id",
            "per_page": per_page,
            "page": page,
        }
        res = request_with_refresh("GET", url, params=params)
        data = res.json()

        if "data" not in data:
            break

        records = data["data"]
        ids.extend(r["id"] for r in records)
        print(f"  {module}: fetched page {page}, total so far: {len(ids)}")

        if not data.get("info", {}).get("more_records", False):
            break

        page += 1
        time.sleep(0.2)

    return ids

# ==============
# FETCH TIMELINE FOR ONE RECORD
# ==============
def get_record_timeline(module: str, record_id: str) -> list[dict]:
    """
    Fetch the complete timeline for a single record.
    First page uses per_page; subsequent pages use page_token.
    """
    timeline = []
    url = f"{ZOHO_DOMAIN}/crm/v6/{module}/{record_id}/__timeline"
    params = {
        "per_page": 200,
        "include_inner_details": (
            "field_history.data_type,field_history.field_label,"
            "done_by.type__s,done_by.profile"
        ),
    }

    while True:
        res = request_with_refresh("GET", url, params=params)
        data = res.json()

        entries = data.get("__timeline", [])
        timeline.extend(entries)

        next_token = data.get("info", {}).get("next_page_token")
        if not next_token:
            break

        # For subsequent pages, switch to page_token (per_page is not allowed)
        params = {"page_token": next_token}
        time.sleep(0.1)

    return timeline

# ==============
# FIELD FILTER
# ==============
def filter_by_field(timeline: list[dict], field_api_name: str) -> list[dict]:
    """Return only timeline entries where a specific field was changed."""
    return [
        entry for entry in timeline
        if entry.get("field_history") and
           any(f["api_name"] == field_api_name for f in entry["field_history"])
    ]

def fetch_field_changes_all_accounts(field_api_name: str, output_file: str) -> list[dict]:
    """
    Fetch Account_Status (or any field) changes across every account.
    Returns a flat list of matching timeline entries enriched with the account_id.
    """
    print(f"\n=== Fetching '{field_api_name}' changes for all Accounts ===")

    record_ids = get_all_record_ids("Accounts")
    print(f"Found {len(record_ids)} accounts.")

    results = []
    total = len(record_ids)

    for i, record_id in enumerate(record_ids, start=1):
        print(f"  [{i}/{total}] Checking {record_id}...")
        try:
            timeline = get_record_timeline("Accounts", record_id)
            matches = filter_by_field(timeline, field_api_name)
            for entry in matches:
                entry["account_id"] = record_id
            results.extend(matches)
        except Exception as exc:
            print(f"    Error: {exc}")

    print(f"Found {len(results)} '{field_api_name}' change(s) across all accounts.")

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"Saved to {output_file}")

    return results

# ==============
# FETCH ALL TIMELINES FOR A MODULE
# ==============
def fetch_all_timelines(module: str, output_file: str) -> dict:
    """Fetch timelines for every record in a module and save to a JSON file."""
    print(f"\n=== Fetching all {module} timelines ===")

    print(f"Step 1: collecting all {module} IDs...")
    record_ids = get_all_record_ids(module)
    print(f"Found {len(record_ids)} {module} record(s).")

    all_timelines = {}
    total = len(record_ids)

    for i, record_id in enumerate(record_ids, start=1):
        print(f"  [{i}/{total}] Timeline for {record_id}...")
        try:
            all_timelines[record_id] = get_record_timeline(module, record_id)
        except Exception as exc:
            print(f"    Error: {exc}")
            all_timelines[record_id] = []

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_timelines, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(all_timelines)} timelines to {output_file}")
    return all_timelines

# ==============
# MAIN
# ==============
if __name__ == "__main__":
    refresh_access_token()

    # ── TEST: single account ───────────────────────────────────────────────
    TEST_ACCOUNT_ID = "386758000054300816"

    print(f"\n=== TEST: timeline for account {TEST_ACCOUNT_ID} ===")
    timeline = get_record_timeline("Accounts", TEST_ACCOUNT_ID)
    print(f"Got {len(timeline)} timeline entries")

    with open("test_account_timeline.json", "w", encoding="utf-8") as f:
        json.dump(timeline, f, indent=2, ensure_ascii=False)
    print("Saved to test_account_timeline.json")

    # ── Filter Account_Status changes for the test account ────────────────
    status_changes = filter_by_field(timeline, "Account_Status")
    print(f"Account_Status changes: {len(status_changes)}")
    for change in status_changes:
        fh = next(f for f in change["field_history"] if f["api_name"] == "Account_Status")
        print(f"  {change['audited_time']}  {fh['_value']['old']} → {fh['_value']['new']}  (by {change['done_by']['name']})")

    # ── FULL EXPORT (uncomment to run) ────────────────────────────────────
    # fetch_all_timelines("Accounts", "accounts_timelines.json")
    # fetch_all_timelines("Deals", "deals_timelines.json")

    # ── Account_Status changes across ALL accounts (uncomment to run) ─────
    # fetch_field_changes_all_accounts("Account_Status", "account_status_changes.json")
