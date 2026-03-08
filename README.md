# Red Alert Tracker — Israel

Web dashboard that tracks and visualizes Israeli red alert (Pikud HaOref) data since February 28, 2026.

## Features

- Fetches historical alert data from Pikud HaOref's API (requires Israeli IP)
- Total alerts, daily trends, peak days
- Most affected cities ranking
- Hourly distribution patterns
- Alert type breakdown
- Recent alerts table
- Manual data refresh

## Setup

```bash
pip install -r requirements.txt
```

## Run

```bash
uvicorn backend.main:app --reload
```

Open [http://localhost:8000](http://localhost:8000).

On first launch, the app fetches all alerts from Feb 28, 2026 to today. This takes a few seconds. Subsequent launches use the cached SQLite database and only fetch new data.

## Data Source

[Pikud HaOref](https://www.oref.org.il/) historical alerts API. Accessible from Israeli IPs only.
