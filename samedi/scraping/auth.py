import os
from playwright.sync_api import Browser, Page

SESSIONS_DIR = os.path.join(os.path.dirname(__file__), "sessions")


def _session_path(city: str) -> str:
    os.makedirs(SESSIONS_DIR, exist_ok=True)
    return os.path.join(SESSIONS_DIR, f"session_{city.lower()}.json")


def login(browser: Browser, account: dict) -> Page:
    """Return an authenticated page, resuming a saved session if available."""
    city = account["city"]
    username = account["username"]
    password = account["password"]
    session_file = _session_path(city)

    # if os.path.exists(session_file):
    #     context = browser.new_context(storage_state=session_file)
    #     page = context.new_page()
    #     page.goto("https://app.samedi.de/start#calendar")
    #     if "login" not in page.url:
    #         return page
    #     context.close()

    context = browser.new_context()
    page = context.new_page()
    page.goto("https://app.samedi.de/login")
    page.fill('input[name="user[pseudonym]"]', username)
    page.fill('input[name="user[password]"]', password)
    page.click('input[name="commit"]')
    page.wait_for_url("https://app.samedi.de/start#calendar")
    context.storage_state(path=session_file)
    return page


def logout(page: Page, city: str):
    """Click the user menu, then log out — handling the optional NPS survey overlay."""
    page.click('svg[data-icon="circle-user"]')
    page.click('button:has-text("Ausloggen")')
    try:
        page.wait_for_selector('#npsAndLogoutOverlay[style*="display: block"]', timeout=3_000)
        page.click('#npsAndLogoutOverlay button:has-text("Abmelden")')
    except Exception:
        pass  # NPS overlay only appears once every 6 weeks
    page.wait_for_url("**/login**", timeout=10_000)
