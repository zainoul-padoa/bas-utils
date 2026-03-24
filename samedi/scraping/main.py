import csv
import json
import os
from datetime import datetime
from playwright.sync_api import sync_playwright

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, MofNCompleteColumn, TimeElapsedColumn
from rich.table import Table
from rich import box

from auth import login, logout
from scraper import scrape_patients

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "config.json")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "../../")

FIELDNAMES = ["city", "last_name", "first_name", "phone", "mobile", "email", "address", "birthdate"]

console = Console()


def load_config() -> list[dict]:
    with open(CONFIG_FILE, encoding="utf-8") as f:
        return json.load(f)


def main():
    accounts = load_config()
    summary = []  # [(city, count)]

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_path = os.path.abspath(os.path.join(OUTPUT_DIR, f"patients_{timestamp}.csv"))

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)

            for account in accounts:
                city = account["city"]
                city_total = 0

                with Progress(
                    SpinnerColumn(),
                    TextColumn("[bold cyan]{task.description}"),
                    BarColumn(bar_width=30),
                    MofNCompleteColumn(),
                    TimeElapsedColumn(),
                    console=console,
                    transient=True,
                ) as progress:
                    task = progress.add_task(
                        f"{city} › logging in...",
                        total=None,  # unknown until page loads
                    )

                    try:
                        page = login(browser, account)

                        progress.update(task, description=f"{city} › scraping...")
                        patient_gen = scrape_patients(page, city)

                        total_patients = next(patient_gen)  # first yield is the count
                        if total_patients is not None:
                            progress.update(task, total=total_patients)

                        for batch in patient_gen:
                            writer.writerows(batch)
                            f.flush()
                            city_total += len(batch)
                            progress.advance(task, len(batch))

                        progress.update(task, description=f"{city} › logging out...")
                        logout(page, city)
                        page.context.close()

                    except Exception as e:
                        console.print(f"[bold red][{city}] ERROR:[/bold red] {e}")
                        try:
                            page.context.close()
                        except Exception:
                            pass

                summary.append((city, city_total))
                console.print(f"[green]✓[/green] [bold]{city}[/bold] — {city_total} patients")

            browser.close()

    total = sum(c for _, c in summary)

    table = Table(title="Scraping Summary", box=box.ROUNDED, show_footer=True)
    table.add_column("City", style="cyan")
    table.add_column("Patients", justify="right", style="green", footer=f"[bold]{total}[/bold]")
    for city, count in summary:
        table.add_row(city, str(count))

    console.print()
    console.print(table)
    console.print(f"\n[dim]Saved to:[/dim] [bold]{output_path}[/bold]")


if __name__ == "__main__":
    main()
