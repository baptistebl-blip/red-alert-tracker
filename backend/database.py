import sqlite3
import os
import json
from datetime import datetime
from typing import Optional

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "alerts.db")
CITIES_MAP_PATH = os.path.join(os.path.dirname(__file__), "cities_map.json")
CITIES_FULL_PATH = os.path.join(os.path.dirname(__file__), "cities_full.json")

CATEGORY_EN = {
    "ירי רקטות וטילים": "Rockets & Missiles",
    "ירי רקטות וטילים -  האירוע הסתיים": "Rockets & Missiles — Ended",
    "חדירת כלי טיס עוין": "Hostile Aircraft Intrusion",
    "חדירת כלי טיס עוין - האירוע הסתיים": "Hostile Aircraft — Ended",
    "חדירת מחבלים": "Terrorist Infiltration",
    "חדירת מחבלים -  החשש הוסר": "Terrorist Infiltration — Cleared",
    "הסתיים אירוע חדירת מחבלים - ניתן לצאת מהבתים": "Infiltration Ended — Safe to Exit",
    "האירוע הסתיים": "Event Ended",
    "הישמעו להנחיות פיקוד העורף": "Follow Home Front Command Instructions",
    "שהייה בסמיכות למרחב מוגן": "Stay Near Shelter",
    "ניתן לצאת מהמרחב המוגן אך יש להישאר בקרבתו": "May Leave Shelter — Stay Nearby",
    "בדקות הקרובות צפויות להתקבל התרעות באזורך": "Alerts Expected in Your Area Soon",
    "עדכון": "Update",
}

_cities_he_to_en: dict[str, str] = {}
_cities_full: dict[str, dict] = {}


def _load_city_map():
    global _cities_he_to_en
    if _cities_he_to_en:
        return
    if os.path.exists(CITIES_MAP_PATH):
        with open(CITIES_MAP_PATH, "r", encoding="utf-8") as f:
            _cities_he_to_en = json.load(f)


def _load_cities_full():
    global _cities_full
    if _cities_full:
        return
    if os.path.exists(CITIES_FULL_PATH):
        with open(CITIES_FULL_PATH, "r", encoding="utf-8") as f:
            _cities_full = json.load(f)


def city_to_en(he_name: str) -> str:
    _load_city_map()
    return _cities_he_to_en.get(he_name, he_name)


def _city_info(he_name: str) -> dict:
    _load_cities_full()
    return _cities_full.get(he_name, {})


def cat_to_en(he_cat: str) -> str:
    return CATEGORY_EN.get(he_cat, he_cat)


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rid TEXT,
            city TEXT NOT NULL,
            alert_date TEXT NOT NULL,
            date_only TEXT NOT NULL,
            time_only TEXT NOT NULL,
            hour INTEGER NOT NULL,
            category INTEGER,
            category_desc TEXT,
            UNIQUE(rid, city)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_date ON alerts(date_only)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_city ON alerts(city)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_hour ON alerts(hour)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_rid ON alerts(rid)")
    conn.commit()
    conn.close()


def insert_alerts_batch(alerts: list[dict]) -> int:
    conn = get_connection()
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA temp_store = MEMORY")
    cur = conn.cursor()
    cur.executemany(
        """INSERT OR IGNORE INTO alerts
           (rid, city, alert_date, date_only, time_only, hour, category, category_desc)
           VALUES (:rid, :city, :alert_date, :date_only, :time_only, :hour, :category, :category_desc)""",
        alerts,
    )
    inserted = cur.rowcount
    conn.commit()
    conn.close()
    return inserted


THREAT_CATEGORIES = (1, 2, 10)


def _where(city: Optional[str]) -> tuple[str, tuple]:
    clauses = ["category IN (1, 2, 10)"]
    params: list = []
    if city:
        clauses.append("city = ?")
        params.append(city)
    return "WHERE " + " AND ".join(clauses), tuple(params)


def _fmt_duration(seconds: float) -> str:
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60}s"
    h = s // 3600
    m = (s % 3600) // 60
    if h < 24:
        return f"{h}h {m}m"
    d = h // 24
    h = h % 24
    return f"{d}d {h}h {m}m"


def _get_timing_stats(cur, w: str, params: tuple) -> dict:
    """Compute time-since-last, avg gap, and longest quiet period.
    Uses distinct alert_date values so simultaneous multi-city alerts count as one event."""
    rows = cur.execute(
        f"SELECT DISTINCT alert_date FROM alerts {w} ORDER BY alert_date", params
    ).fetchall()

    if not rows:
        return {"since_last": None, "avg_gap": None, "longest_quiet": None}

    timestamps = []
    for r in rows:
        try:
            timestamps.append(datetime.fromisoformat(r["alert_date"]))
        except ValueError:
            pass

    last_iso = timestamps[-1].strftime("%Y-%m-%dT%H:%M:%S+03:00")

    if len(timestamps) < 2:
        return {
            "last_alert_iso": last_iso,
            "avg_gap": None,
            "longest_quiet": None,
        }

    gaps = [(timestamps[i + 1] - timestamps[i]).total_seconds() for i in range(len(timestamps) - 1)]
    avg_gap = sum(gaps) / len(gaps)
    max_gap = max(gaps)
    max_gap_idx = gaps.index(max_gap)
    quiet_start = timestamps[max_gap_idx]
    quiet_end = timestamps[max_gap_idx + 1]

    return {
        "last_alert_iso": last_iso,
        "avg_gap": _fmt_duration(avg_gap),
        "avg_gap_seconds": avg_gap,
        "longest_quiet": _fmt_duration(max_gap),
        "longest_quiet_seconds": max_gap,
        "longest_quiet_from": quiet_start.strftime("%Y-%m-%d %H:%M"),
        "longest_quiet_to": quiet_end.strftime("%Y-%m-%d %H:%M"),
    }


def get_summary(city: Optional[str] = None) -> dict:
    conn = get_connection()
    cur = conn.cursor()
    w, params = _where(city)

    total = cur.execute(f"SELECT COUNT(*) FROM alerts {w}", params).fetchone()[0]
    today = datetime.now().strftime("%Y-%m-%d")

    tw, tp = _where(city)
    today_count = cur.execute(
        f"SELECT COUNT(*) FROM alerts {tw} AND date_only = ?" if city
        else f"SELECT COUNT(*) FROM alerts {tw} AND date_only = ?",
        (*tp, today),
    ).fetchone()[0]

    peak = cur.execute(
        f"SELECT date_only, COUNT(*) as cnt FROM alerts {w} GROUP BY date_only ORDER BY cnt DESC LIMIT 1", params
    ).fetchone()
    peak_day = {"date": peak["date_only"], "count": peak["cnt"]} if peak else None

    if city:
        most_affected = {"city": city_to_en(city), "count": total}
    else:
        wt, pt = _where(None)
        top_city = cur.execute(
            f"SELECT city, COUNT(*) as cnt FROM alerts {wt} GROUP BY city ORDER BY cnt DESC LIMIT 1", pt
        ).fetchone()
        most_affected = {"city": city_to_en(top_city["city"]), "count": top_city["cnt"]} if top_city else None

    first = cur.execute(f"SELECT date_only FROM alerts {w} ORDER BY date_only ASC LIMIT 1", params).fetchone()
    last = cur.execute(f"SELECT date_only FROM alerts {w} ORDER BY date_only DESC LIMIT 1", params).fetchone()

    total_days = 0
    if first and last:
        d1 = datetime.strptime(first["date_only"], "%Y-%m-%d")
        d2 = datetime.strptime(last["date_only"], "%Y-%m-%d")
        total_days = (d2 - d1).days + 1

    timing = _get_timing_stats(cur, w, params)

    conn.close()
    return {
        "total_alerts": total,
        "today_alerts": today_count,
        "peak_day": peak_day,
        "most_affected_city": most_affected,
        "total_days": total_days,
        "avg_per_day": round(total / total_days, 1) if total_days > 0 else 0,
        **timing,
    }


def get_daily_stats(city: Optional[str] = None) -> list[dict]:
    conn = get_connection()
    w, params = _where(city)
    rows = conn.execute(
        f"SELECT date_only, COUNT(*) as cnt FROM alerts {w} GROUP BY date_only ORDER BY date_only", params
    ).fetchall()
    conn.close()
    return [{"date": r["date_only"], "count": r["cnt"]} for r in rows]


def get_city_stats(limit: int = 30, city: Optional[str] = None) -> list[dict]:
    conn = get_connection()
    w, params = _where(city)
    rows = conn.execute(
        f"SELECT city, COUNT(*) as cnt FROM alerts {w} GROUP BY city ORDER BY cnt DESC LIMIT ?",
        (*params, limit),
    ).fetchall()
    conn.close()
    return [{"city": city_to_en(r["city"]), "count": r["cnt"]} for r in rows]


def get_hourly_stats(city: Optional[str] = None) -> list[dict]:
    conn = get_connection()
    w, params = _where(city)
    rows = conn.execute(
        f"SELECT hour, COUNT(*) as cnt FROM alerts {w} GROUP BY hour ORDER BY hour", params
    ).fetchall()
    conn.close()
    result = {h: 0 for h in range(24)}
    for r in rows:
        result[r["hour"]] = r["cnt"]
    return [{"hour": h, "count": c} for h, c in sorted(result.items())]


def get_category_stats(city: Optional[str] = None) -> list[dict]:
    conn = get_connection()
    w, params = _where(city)
    rows = conn.execute(
        f"SELECT category_desc, COUNT(*) as cnt FROM alerts {w} GROUP BY category_desc ORDER BY cnt DESC", params
    ).fetchall()
    conn.close()
    return [{"category": cat_to_en(r["category_desc"]), "count": r["cnt"]} for r in rows]


def get_recent_alerts(limit: int = 50, city: Optional[str] = None) -> list[dict]:
    conn = get_connection()
    w, params = _where(city)
    rows = conn.execute(
        f"SELECT city, alert_date, date_only, time_only, category_desc FROM alerts {w} ORDER BY alert_date DESC LIMIT ?",
        (*params, limit),
    ).fetchall()
    conn.close()
    return [
        {
            "city": city_to_en(r["city"]),
            "alert_date": r["alert_date"],
            "date": r["date_only"],
            "time": r["time_only"],
            "category": cat_to_en(r["category_desc"]),
        }
        for r in rows
    ]


_all_cities_cache: list[tuple[str, str]] = []


def _get_all_cities() -> list[tuple[str, str]]:
    global _all_cities_cache
    if _all_cities_cache:
        return _all_cities_cache
    _load_city_map()
    conn = get_connection()
    rows = conn.execute("SELECT DISTINCT city FROM alerts ORDER BY city").fetchall()
    conn.close()
    _all_cities_cache = [(r["city"], city_to_en(r["city"])) for r in rows]
    return _all_cities_cache


def search_cities(query: str, limit: int = 20) -> list[dict]:
    q_lower = query.lower()
    results = []
    for he, en in _get_all_cities():
        if q_lower in he.lower() or q_lower in en.lower():
            results.append({"he": he, "en": en})
        if len(results) >= limit:
            break
    return results


def get_geo_stats(city: Optional[str] = None) -> list[dict]:
    _load_cities_full()
    conn = get_connection()
    w, params = _where(city)
    rows = conn.execute(
        f"SELECT city, COUNT(*) as cnt FROM alerts {w} GROUP BY city ORDER BY cnt DESC", params
    ).fetchall()
    conn.close()

    results = []
    for r in rows:
        info = _city_info(r["city"])
        lat = info.get("lat")
        lng = info.get("lng")
        if lat and lng:
            results.append({
                "city": info.get("en", r["city"]),
                "lat": lat,
                "lng": lng,
                "count": r["cnt"],
            })
    return results


def get_region_stats(city: Optional[str] = None) -> list[dict]:
    _load_cities_full()
    conn = get_connection()
    w, params = _where(city)
    rows = conn.execute(
        f"SELECT city, COUNT(*) as cnt FROM alerts {w} GROUP BY city", params
    ).fetchall()
    conn.close()

    region_counts: dict[str, int] = {}
    for r in rows:
        info = _city_info(r["city"])
        region = info.get("area", "Unknown")
        region_counts[region] = region_counts.get(region, 0) + r["cnt"]

    return sorted(
        [{"region": k, "count": v} for k, v in region_counts.items()],
        key=lambda x: x["count"],
        reverse=True,
    )


def get_clearance_stats(city: Optional[str] = None) -> dict:
    """Compute time between a threat alert and its corresponding all-clear for each city."""
    conn = get_connection()
    city_clause = "AND city = ?" if city else ""
    city_params = (city,) if city else ()

    rows = conn.execute(
        f"""SELECT city, alert_date, category FROM alerts
            WHERE category IN (1, 2, 10, 13)
            {city_clause}
            ORDER BY city, alert_date""",
        city_params,
    ).fetchall()
    conn.close()

    CLEAR_DESCS = {13}
    THREAT_CATS = {1, 2, 10}

    durations = []
    current_city = None
    threat_time = None

    for r in rows:
        c = r["city"]
        cat = r["category"]
        try:
            ts = datetime.fromisoformat(r["alert_date"])
        except ValueError:
            continue

        if c != current_city:
            current_city = c
            threat_time = None

        if cat in THREAT_CATS:
            if threat_time is None:
                threat_time = ts
        elif cat in CLEAR_DESCS and threat_time is not None:
            gap = (ts - threat_time).total_seconds()
            if 0 < gap < 7200:
                durations.append(gap)
            threat_time = None

    if not durations:
        return {"avg": None, "median": None, "min": None, "max": None, "count": 0}

    durations.sort()
    n = len(durations)
    median = durations[n // 2] if n % 2 else (durations[n // 2 - 1] + durations[n // 2]) / 2

    return {
        "avg": _fmt_duration(sum(durations) / n),
        "avg_seconds": sum(durations) / n,
        "median": _fmt_duration(median),
        "median_seconds": median,
        "min": _fmt_duration(min(durations)),
        "min_seconds": min(durations),
        "max": _fmt_duration(max(durations)),
        "max_seconds": max(durations),
        "count": n,
    }


def get_last_fetched_date() -> Optional[str]:
    conn = get_connection()
    row = conn.execute("SELECT date_only FROM alerts ORDER BY date_only DESC LIMIT 1").fetchone()
    conn.close()
    return row["date_only"] if row else None


def get_alert_count() -> int:
    conn = get_connection()
    count = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
    conn.close()
    return count
