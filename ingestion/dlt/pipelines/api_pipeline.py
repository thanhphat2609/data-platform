"""
Events ingestion pipeline → Azure Blob bronze/dlt/events/

Sources:
  - USGS Earthquake Events  (incremental by time, free — no key required)
  - Open-Meteo Hourly       (incremental by date, free — no key required)
  - Exchange Rates           (incremental by date, fallback to frankfurter.app)

Output path: az://bronze/dlt/events/<stream>/ingestion_date=<date>/<load_id>.<file_id>.parquet
"""
from datetime import date, datetime, timedelta
from typing import Iterator
import os

import dlt
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

OPENMETEO_LAT = float(os.getenv("OPENMETEO_LAT", "10.8231"))
OPENMETEO_LON = float(os.getenv("OPENMETEO_LON", "106.6297"))
EXCHANGERATES_API_KEY = os.getenv("EXCHANGERATES_API_KEY", "")
USGS_MIN_MAGNITUDE = float(os.getenv("USGS_MIN_MAGNITUDE", "1.0"))
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "30"))


def _http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


@dlt.source(name="events")
def events_source():
    return [earthquake_events(), openmeteo_hourly(), exchange_rates()]


@dlt.resource(name="earthquake_events", write_disposition="append")
def earthquake_events(
    time_cursor: dlt.sources.incremental[str] = dlt.sources.incremental(
        "time", initial_value=str(date.today() - timedelta(days=LOOKBACK_DAYS))
    ),
) -> Iterator[dict]:
    session = _http_session()
    start_time = str(time_cursor.last_value)[:10]
    end_time = date.today().isoformat()

    resp = session.get(
        "https://earthquake.usgs.gov/fdsnws/event/1/query",
        params={
            "format": "geojson",
            "starttime": start_time,
            "endtime": end_time,
            "minmagnitude": USGS_MIN_MAGNITUDE,
            "orderby": "time-asc",
            "limit": 20000,
        },
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()

    for feature in data.get("features", []):
        props = feature["properties"]
        coords = feature["geometry"]["coordinates"]
        yield {
            "id": feature["id"],
            "time": datetime.utcfromtimestamp(props["time"] / 1000).isoformat(),
            "magnitude": props.get("mag"),
            "place": props.get("place"),
            "type": props.get("type"),
            "longitude": coords[0],
            "latitude": coords[1],
            "depth_km": coords[2],
            "status": props.get("status"),
            "tsunami": props.get("tsunami"),
            "significance": props.get("sig"),
        }


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
