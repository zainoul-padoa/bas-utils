
import time
import requests
import json

# ==============
# CONFIG
# ==============
ZOHO_DOMAIN = "https://www.zohoapis.eu"  # change to .eu, .in, .com.au if needed
CLIENT_ID = '1000.UM9NBC0TBJ49IQXT7M6YLDP3OOL5AB'
CLIENT_SECRET =  ''
REFRESH_TOKEN = ""

ACCESS_TOKEN = None  # will be filled dynamically
LIST_MODULES = [
                "Leads", "Accounts", "Contacts", "Deals", "Campaigns",
                "Tasks", 
                # "Cases", -> ça marche pas
                 "Events", "Calls", 
                # "Solutions", -> ça marche pas
                 "Products",
                # "Vendors",  -> ça marche pas
                # "Pricebooks",  -> ça marche pas
                # "Quotes",  -> ça marche pas
                 "Salesorders", 
                # "Purchaseorders",  -> ça marche pas
                "Invoices"]

# ==============
# AUTH
# ==============
def refresh_access_token():
    """
    Refresh Zoho OAuth access token using the refresh token.
    """
    global ACCESS_TOKEN
    url = f"https://accounts.zoho.eu/oauth/v2/token"
    params = {
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token"
    }
    res = requests.post(url, params=params)
    res.raise_for_status()
    data = res.json()
    print(data)
    ACCESS_TOKEN = data["access_token"]
    print("✅ Access token refreshed")
    return ACCESS_TOKEN

def zoho_headers():
    if not ACCESS_TOKEN:
        refresh_access_token()
    return {
        "Authorization": f"Zoho-oauthtoken {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

def request_with_refresh(method, url, **kwargs):
    """
    Wrapper around requests to auto-refresh token on 401 errors.
    """
    res = requests.request(method, url, headers=zoho_headers(), **kwargs)
    if res.status_code == 401:  # token expired
        print("⚠️ Access token expired, refreshing...")
        refresh_access_token()
        res = requests.request(method, url, headers=zoho_headers(), **kwargs)
    res.raise_for_status()
    return res

# ==============
# BULK HELPERS
# ==============
def create_bulk_export(module: str, fields: list = None):
    url = f"{ZOHO_DOMAIN}/crm/bulk/v2/read"
    body = {
        "query": {
            "module": module
        }
    }
    res = request_with_refresh("POST", url, json=body)
    print(res.json()["data"][0])
    return res.json()["data"][0]["details"]["id"]

def poll_bulk_status(job_id: str, interval: int = 5):
    url = f"{ZOHO_DOMAIN}/crm/bulk/v2/read/{job_id}"
    while True:
        res = request_with_refresh("GET", url)
        data = res.json()["data"][0]
        print(f"Job {job_id} status: {data['state']}")
        if data["state"] in ("COMPLETED", "FAILED"):
            return data
        time.sleep(interval)

def download_bulk_result(download_url: str, filename: str = "exportZoho.zip"):
    print(f"{ZOHO_DOMAIN}"+download_url+" is the correct download url")
    res = request_with_refresh("GET", f"{ZOHO_DOMAIN}"+download_url)
    with open(filename, "wb") as f:
        for chunk in res.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"✅ Result saved to {filename}")

# ==============
# MAIN FLOW
# ==============
if __name__ == "__main__":
    for MODULE in LIST_MODULES:
     # MODULE = "Leads"  # Example module
        print("Starting to work on module " + MODULE)
        # Step 1: Create export job
        job_id = create_bulk_export(MODULE)
        print(f"Created job {job_id}")

        # Step 2: Poll until complete
        job_status = poll_bulk_status(job_id)

        if job_status["state"] == "COMPLETED":
            file_url = job_status["result"]["download_url"]
            # Step 3: Download result
            print(file_url + " : download available")
            download_bulk_result(file_url, f"{MODULE}_exportZoho.zip")
        else:
            print(f"❌ Job failed: {json.dumps(job_status, indent=2)}")

# ==============
# Fichier de sarah

# DANS LE NAVIGATEUR : 

# https://accounts.zoho.eu/oauth/v2/auth
# ?scope=ZohoCRM.bulk.read,ZohoCRM.modules.ALL
# &client_id=1000.UM9NBC0TBJ49IQXT7M6YLDP3OOL5AB
# &response_type=code
# &access_type=offline
# &redirect_uri=https://localhost/oauth/callback

# -> Retourne : 

# code=1000.9ea5c1b943fc1790bb05ee69552fc941.13596200e08c351f027299e4e6b5cc83
# location=eu
# accounts-server=https%3A%2F%2Faccounts.zoho.eu

# curl -X POST "https://accounts.zoho.eu/oauth/v2/token" \
# -d "code=1000.9ea5c1b943fc1790bb05ee69552fc941.13596200e08c351f027299e4e6b5cc83" \
# -d "client_id=1000.UM9NBC0TBJ49IQXT7M6YLDP3OOL5AB" \
# -d "client_secret=adeb48f856682312a3e223119073c4b09e2ea15a24" \
# -d "redirect_uri=https://localhost/oauth/callback" \
# -d "grant_type=authorization_code"

# {"access_token":"1000.b4020041766dbfe14b3379433b5755b6.02f6eb8b6b7a7d2bec1a086d46482166",
# "refresh_token":"1000.c3f7aaf48e0006a124c2d685494de942.1d2c3ce0a12ed4b3ac731b8fce800451",
# "scope":"ZohoCRM.bulk.read ZohoCRM.modules.ALL",
# "api_domain":"https://www.zohoapis.eu",
# "token_type":"Bearer",
# "expires_in":3600}


# curl -X POST "https://www.zohoapis.eu/crm/bulk/v2/read" \
# -H "Authorization: Zoho-oauthtoken 1000.b4020041766dbfe14b3379433b5755b6.02f6eb8b6b7a7d2bec1a086d46482166" \
# -H "Content-Type: application/json" \
# -d '{
#     "query": {
#         "module": "Accounts",
#         "page": 1
#     }
# }'

# curl -X POST "https://www.zohoapis.eu/crm/bulk/v2/read" \
# -H "Authorization: Zoho-oauthtoken 1000.a0d4ceae40ec5bf520cd4ec003f5fffb.b2d92f983073b85c310afb70482e194f" \
# -H "Content-Type: application/json" \
# -d '{
#     "query": {
#         "module": "Accounts",
#         "fields": ["Account_Name","Website","Phone","Owner","Created_Time"],
#         "page": 1
#     }
# }'

# curl -L "https://www.zohoapis.eu/crm/bulk/v2/read/386758000060915086/result" \
# -H "Authorization: Zoho-oauthtoken 1000.b4020041766dbfe14b3379433b5755b6.02f6eb8b6b7a7d2bec1a086d46482166" \
# -o accounts.zip

