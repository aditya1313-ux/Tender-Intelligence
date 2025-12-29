import csv
from datetime import datetime
from pathlib import Path

# ------------------ PROJECT ROOT ------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]

TENDER_RESULTS_DIR = PROJECT_ROOT / "tender_results"
TENDER_RESULTS_DIR.mkdir(exist_ok=True)
# --------------------------------------------------


STANDARD_FIELDS = [
    "portal",
    "tdr_no",
    "title",
    "description",
    "tender_value",
    "state",
    "city",
    "closing_date",
    "detail_url",
    "pdf_urls",
    "crawled_at"
]


def save_tenders_to_csv(tenders, portal):
    """
    Saves scraped tenders to tender_results/<portal>_<timestamp>.csv
    Always uses absolute project-root path.
    Returns absolute file path.
    """

    filename = f"{portal}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    path = TENDER_RESULTS_DIR / filename

    cleaned_rows = []
    for t in tenders:
        row = {}
        for field in STANDARD_FIELDS:
            row[field] = t.get(field, "")
        cleaned_rows.append(row)

    with open(path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=STANDARD_FIELDS)
        writer.writeheader()
        writer.writerows(cleaned_rows)

    return str(path.resolve())
