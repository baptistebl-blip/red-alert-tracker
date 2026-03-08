import httpx
import csv
import io
import os
from datetime import datetime
from backend.database import insert_alerts_batch, get_alert_count

CSV_URL = "https://raw.githubusercontent.com/dleshem/israel-alerts-data/main/israel-alerts.csv"
START_DATE = "2026-02-28"
CSV_CACHE = os.path.join(os.path.dirname(__file__), "..", "israel-alerts-raw.csv")


def _parse_alert_date(raw: str) -> tuple[str, str, int]:
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        dt = datetime.strptime(raw, "%Y-%m-%dT%H:%M:%S")
    return dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M:%S"), dt.hour


def _download_csv() -> str:
    """Download CSV from GitHub. Uses local cache if less than 1 hour old."""
    if os.path.exists(CSV_CACHE):
        age = datetime.now().timestamp() - os.path.getmtime(CSV_CACHE)
        if age < 3600:
            print(f"Using cached CSV ({int(age)}s old)")
            with open(CSV_CACHE, "r", encoding="utf-8") as f:
                return f.read()

    print("Downloading alerts CSV from GitHub...")
    resp = httpx.get(CSV_URL, timeout=120, follow_redirects=True)
    resp.raise_for_status()
    text = resp.text
    with open(CSV_CACHE, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"Downloaded {len(text):,} bytes")
    return text


def fetch_all_alerts() -> int:
    """Download CSV, parse, filter from START_DATE, insert into DB."""
    csv_text = _download_csv()
    reader = csv.DictReader(io.StringIO(csv_text))

    batch = []
    skipped = 0
    for row in reader:
        alert_date_str = row.get("alertDate", "")
        if not alert_date_str or alert_date_str < f"{START_DATE}T00:00:00":
            skipped += 1
            continue

        date_only, time_only, hour = _parse_alert_date(alert_date_str)
        city = row.get("data", "").strip()
        if not city:
            continue

        batch.append({
            "rid": row.get("rid", ""),
            "city": city,
            "alert_date": alert_date_str,
            "date_only": date_only,
            "time_only": time_only,
            "hour": hour,
            "category": row.get("category"),
            "category_desc": row.get("category_desc", ""),
        })

    print(f"Parsed {len(batch):,} alerts (skipped {skipped:,} before {START_DATE})")
    inserted = insert_alerts_batch(batch)
    print(f"Inserted {inserted:,} new rows into DB")
    return inserted
