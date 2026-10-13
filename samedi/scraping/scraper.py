from playwright.sync_api import Page


def _get_cell_text(row, td_class: str) -> str:
    el = row.query_selector(f".{td_class} .x-grid3-cell-inner")
    if not el:
        return ""
    text = el.inner_text().strip()
    return "" if text == "\u00a0" else text  # strip &nbsp;


def _get_email(row) -> str:
    el = row.query_selector(".x-grid3-td-7 a")
    return el.get_attribute("href").replace("mailto:", "") if el else ""


def _has_next_page(page: Page) -> bool:
    next_btn = page.query_selector(".x-tbar-page-next")
    if not next_btn:
        return False
    parent = next_btn.evaluate_handle("el => el.closest('table')").as_element()
    return "x-item-disabled" not in (parent.get_attribute("class") or "")


def _go_next_page(page: Page):
    first_row_text = page.query_selector(".x-grid3-body .x-grid3-row").inner_text()
    page.click(".x-tbar-page-next")
    page.wait_for_function(
        f"() => document.querySelector('.x-grid3-body .x-grid3-row')?.innerText !== {repr(first_row_text)}"
    )


def _scrape_current_page(page: Page, city: str) -> list[dict]:
    page.wait_for_selector(".x-grid3-body", timeout=10_000)
    rows = page.query_selector_all(".x-grid3-body .x-grid3-row")
    return [
        {
            "city":       city,
            "last_name":  _get_cell_text(row, "x-grid3-td-3"),
            "first_name": _get_cell_text(row, "x-grid3-td-4"),
            "phone":      _get_cell_text(row, "x-grid3-td-5"),
            "mobile":     _get_cell_text(row, "x-grid3-td-6"),
            "email":      _get_email(row),
            "address":    _get_cell_text(row, "x-grid3-td-8"),
            "birthdate":  _get_cell_text(row, "x-grid3-td-9"),
        }
        for row in rows
    ]


def get_total_patients(page: Page) -> int | None:
    """Parse the total patient count from the paging info bar.

    The bar contains text like 'Einträge 1 bis 50 von 13187'.
    Returns None if the element or number can't be found.
    """
    el = page.query_selector(".x-paging-info")
    if not el:
        return None
    text = el.inner_text()
    # text is e.g. "Einträge 1 bis 50 von 13187"
    parts = text.split("von")
    if len(parts) < 2:
        return None
    try:
        return int(parts[-1].strip().replace(".", "").replace(",", ""))
    except ValueError:
        return None


def scrape_patients(page: Page, city: str):
    """Navigate to the patients list and yield one page of patient dicts at a time.

    The first yielded value is the total patient count (int | None), not a batch.
    Subsequent yields are lists of patient dicts.
    """
    page.goto("https://app.samedi.de/start#patients")
    page.wait_for_selector(".x-paging-info", timeout=15_000)

    total = get_total_patients(page)
    yield total  # first yield: total count for progress bar

    while True:
        batch = _scrape_current_page(page, city)
        yield batch
        if not _has_next_page(page):
            break
        _go_next_page(page)
