import csv
import re
import sys
from pathlib import Path

DATE_COLS = ("vertragsbeginn", "start_aktuelle_periode")

MONTHS = {
    "Jan": "01", "Feb": "02", "Mrz": "03", "Apr": "04",
    "Mai": "05", "Jun": "06", "Jul": "07", "Aug": "08",
    "Sep": "09", "Okt": "10", "Nov": "11", "Dez": "12",
}

DATE_RE = re.compile(r"^\s*(\d{1,2})\s*/\s*([A-Za-zäöüÄÖÜ]+)\s+(\d{2,4})\s*$")


def convert(value: str) -> str:
    if not value or value.startswith("#"):
        return value
    m = DATE_RE.match(value)
    if not m:
        return value
    day, mon, year = m.groups()
    if mon not in MONTHS:
        return value
    if len(year) == 2:
        year = "20" + year
    return f"{year}-{MONTHS[mon]}-{int(day):02d}"


def main(src: Path, dst: Path) -> None:
    with src.open(newline="") as fin, dst.open("w", newline="") as fout:
        reader = csv.reader(fin, skipinitialspace=True)
        header = [h.strip() for h in next(reader)]
        date_idx = [i for i, h in enumerate(header) if h in DATE_COLS]
        writer = csv.writer(fout)
        writer.writerow(header)
        for row in reader:
            row = [v.strip() for v in row]
            for i in date_idx:
                if i < len(row):
                    row[i] = convert(row[i])
            writer.writerow(row)


if __name__ == "__main__":
    src = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("full_basic_care.csv")
    dst = Path(sys.argv[2]) if len(sys.argv) > 2 else src.with_name(src.stem + "_normalized.csv")
    main(src, dst)
    print(f"wrote {dst}")
