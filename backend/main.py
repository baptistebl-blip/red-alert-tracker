import threading
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from backend.database import (
    init_db,
    get_summary,
    get_daily_stats,
    get_city_stats,
    get_hourly_stats,
    get_category_stats,
    get_recent_alerts,
    get_alert_count,
    search_cities,
    get_geo_stats,
    get_region_stats,
    get_clearance_stats,
)
from backend.fetcher import fetch_all_alerts

_fetch_in_progress = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    if get_alert_count() == 0:
        print("No alerts in DB. Starting initial data fetch...")
        _start_background_fetch()
    yield


app = FastAPI(title="Red Alert Tracker", lifespan=lifespan)

app.mount("/static", StaticFiles(directory="frontend"), name="static")


def _start_background_fetch():
    global _fetch_in_progress
    if _fetch_in_progress:
        return
    _fetch_in_progress = True
    thread = threading.Thread(target=_run_fetch, daemon=True)
    thread.start()


def _run_fetch():
    global _fetch_in_progress
    try:
        count = fetch_all_alerts()
        print(f"Fetch complete. {count:,} rows inserted.")
    except Exception as e:
        print(f"Fetch error: {e}")
    finally:
        _fetch_in_progress = False


@app.get("/")
async def index():
    return FileResponse("frontend/index.html")


@app.get("/api/stats/summary")
async def stats_summary(city: Optional[str] = Query(None)):
    return get_summary(city)


@app.get("/api/stats/daily")
async def stats_daily(city: Optional[str] = Query(None)):
    return get_daily_stats(city)


@app.get("/api/stats/cities")
async def stats_cities(limit: int = 30, city: Optional[str] = Query(None)):
    return get_city_stats(limit, city)


@app.get("/api/stats/hourly")
async def stats_hourly(city: Optional[str] = Query(None)):
    return get_hourly_stats(city)


@app.get("/api/stats/categories")
async def stats_categories(city: Optional[str] = Query(None)):
    return get_category_stats(city)


@app.get("/api/alerts/recent")
async def alerts_recent(limit: int = 50, city: Optional[str] = Query(None)):
    return get_recent_alerts(limit, city)


@app.get("/api/cities/search")
async def cities_search(q: str = Query("", min_length=1), limit: int = 20):
    return search_cities(q, limit)


@app.get("/api/stats/geo")
async def stats_geo(city: Optional[str] = Query(None)):
    return get_geo_stats(city)


@app.get("/api/stats/regions")
async def stats_regions(city: Optional[str] = Query(None)):
    return get_region_stats(city)


@app.get("/api/stats/clearance")
async def stats_clearance(city: Optional[str] = Query(None)):
    return get_clearance_stats(city)


@app.post("/api/data/refresh")
async def data_refresh():
    if _fetch_in_progress:
        return {"status": "already_running"}
    _start_background_fetch()
    return {"status": "started"}


@app.get("/api/data/status")
async def data_status():
    return {
        "fetching": _fetch_in_progress,
        "total_alerts": get_alert_count(),
    }
