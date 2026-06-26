"""
Events ingestion pipeline → Azure Blob bronze/dlt/events/

Sources:
  - GitHub Events       (incremental by created_at)
  - Open-Meteo Hourly   (incremental by date, free — no key required)
  - Exchange Rates      (incremental by date, fallback to frankfurter.app)

Output path: az://bronze/dlt/events/<stream>/ingestion_date=<date>/<load_id>.<file_id>.parquet
"""
import os
from datetime import date, datetime, timedelta
from typing import Iterator

import dlt
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GITHUB_REPO = os.getenv("GITHUB_REPO", "thanhphat2609/data-platform-ingestion")
OPENMETEO_LAT = float(os.getenv("OPENMETEO_LAT", "10.8231"))
OPENMETEO_LON = float(os.getenv("OPENMETEO_LON", "106.6297"))
EXCHANGERATES_API_KEY = os.getenv("EXCHANGERATES_API_KEY", "")
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "30"))


def _http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


@dlt.source(name="events")
def events_source():
    return [github_events(), openmeteo_hourly(), exchange_rates()]


@dlt.resource(name="github_events", write_disposition="append")
def github_events(
    created_at: dlt.sources.incremental[datetime] = dlt.sources.incremental(
        "created_at", initial_value="2024-01-01T00:00:00"
    ),
) -> Iterator[dict]:
    session = _http_session()
    headers = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

    page = 1
    while True:
        resp = session.get(
            f"https://api.github.com/repos/{GITHUB_REPO}/events",
            headers=headers,
            params={"per_page": 100, "page": page},
            timeout=30,
        )
        resp.raise_for_status()
        events = resp.json()

        if not events:
            break

        for event in events:
            yield {
                "id": event["id"],
                "type": event["type"],
                "actor_login": event["actor"]["login"],
                "repo_name": event["repo"]["name"],
                "public": event.get("public", True),
                "created_at": event["created_at"],
            }

        # GitHub paginates events — stop when we hit older records
        if len(events) < 100:
            break
        page += 1


@dlt.resource(name="openmeteo_hourly", write_disposition="append")
def openmeteo_hourly(
    date_cursor: dlt.sources.incremental[str] = dlt.sources.incremental(
        "date", initial_value=str(date.today() - timedelta(days=LOOKBACK_DAYS))
    ),
) -> Iterator[dict]:
    start_date = str(date_cursor.last_value)[:10]
    end_date = date.today().isoformat()

    session = _http_session()
    resp = session.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": OPENMETEO_LAT,
            "longitude": OPENMETEO_LON,
            "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation",
            "start_date": start_date,
            "end_date": end_date,
            "timezone": "Asia/Ho_Chi_Minh",
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    hourly = data["hourly"]
    for i, dt_str in enumerate(hourly["time"]):
        yield {
            "datetime": dt_str,
            "date": dt_str[:10],
            "latitude": data["latitude"],
            "longitude": data["longitude"],
            "timezone": data.get("timezone", "UTC"),
            "temperature_2m": hourly["temperature_2m"][i],
            "relative_humidity_2m": hourly["relative_humidity_2m"][i],
            "wind_speed_10m": hourly["wind_speed_10m"][i],
            "precipitation": hourly["precipitation"][i],
        }


@dlt.resource(name="exchange_rates", write_disposition="append")
def exchange_rates(
    date_cursor: dlt.sources.incremental[str] = dlt.sources.incremental(
        "date", initial_value=str(date.today() - timedelta(days=LOOKBACK_DAYS))
    ),
) -> Iterator[dict]:
    session = _http_session()
    start = date.fromisoformat(str(date_cursor.last_value)[:10])
    today = date.today()

    current = start
    while current <= today:
        date_str = current.isoformat()

        # Use frankfurter.app as free fallback; override with paid key
        if EXCHANGERATES_API_KEY:
            url = f"https://api.exchangeratesapi.io/v1/{date_str}"
            params = {"access_key": EXCHANGERATES_API_KEY, "base": "USD"}
        else:
            url = f"https://api.frankfurter.app/{date_str}"
            params = {"base": "USD"}

        try:
            resp = session.get(url, params=params, timeout=15)
            resp.raise_for_status()
            payload = resp.json()
            rates = payload.get("rates", {})

            for target_currency, rate in rates.items():
                yield {
                    "date": date_str,
                    "base": payload.get("base", "USD"),
                    "target": target_currency,
                    "rate": float(rate),
                }
        except requests.exceptions.HTTPError:
            pass  # Skip unavailable dates (weekends, holidays)

        current += timedelta(days=1)


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="events_ingestion",
        destination="filesystem",
        dataset_name="events",
    )
    load_info = pipeline.run(events_source())
    print(load_info)
