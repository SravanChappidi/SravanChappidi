# Build guide: all the code, phase by phase

Every file in the project, grouped by the phase where you create it. For each phase: create the files in VS Code (right-click in the Explorer → **New File**, and type the path, e.g. `ingestion/config.py`; VS Code creates the folders), paste the code, then run the commands at the end of the phase.

The *why* behind each phase (explanations, expected output, common errors) is in the matching `docs/phase-XX-*.md` guide.

> **Shortcut:** instead of creating the files by hand, you can get them all at once:
> `git clone -b claude/weather-etl-pipeline-poc-6g06d6 https://github.com/SravanChappidi/SravanChappidi.git`

## Contents

- [Phase 1: Project setup](#phase-1-project-setup)
- [Phase 2: Weather API ingestion](#phase-2-weather-api-ingestion)
- [Phase 3: Kafka producer](#phase-3-kafka-producer)
- [Phase 4: Kafka topic and consumer validation](#phase-4-kafka-topic-and-consumer-validation)
- [Phase 5: Spark Structured Streaming](#phase-5-spark-structured-streaming)
- [Phase 6: Data cleaning and transformation](#phase-6-data-cleaning-and-transformation)
- [Phase 7: Snowflake integration](#phase-7-snowflake-integration)
- [Phase 8: Data validation](#phase-8-data-validation)
- [Phases 9 and 10: Power BI connection and dashboard](#phases-9-and-10-power-bi-connection-and-dashboard)
- [Phases 11 and 12: End-to-end testing and documentation](#phases-11-and-12-end-to-end-testing-and-documentation)

---

## Phase 1: Project setup

Guide: [docs/phase-01-project-setup.md](phase-01-project-setup.md)

Create the project folder and open it in VS Code (File → Open Folder). Create these files:

### `.gitignore`

```gitignore
# Secrets - never commit real credentials
.env

# Python
.venv/
venv/
__pycache__/
*.pyc
.pytest_cache/

# Spark runtime state
checkpoints/
spark-warehouse/
metastore_db/
derby.log
logs/

# Power BI local files (commit screenshots, not large .pbix, unless you want to)
*.pbix.bak

# OS / IDE
.DS_Store
Thumbs.db
.vscode/
.idea/
```

### `.env.example`

```ini
# ---------------------------------------------------------------
# Copy this file to .env and fill in real values:  copy .env.example .env
# .env is git-ignored. Never commit real credentials.
# ---------------------------------------------------------------

# --- Weather API (OpenWeatherMap, free "Current Weather Data" API) ---
WEATHER_API_KEY=replace_with_your_openweathermap_key
WEATHER_API_BASE_URL=https://api.openweathermap.org/data/2.5/weather
WEATHER_UNITS=metric
# How often to poll the API, in seconds. OpenWeatherMap refreshes current
# weather roughly every 10 minutes, so 300 (5 min) is a sensible POC value.
POLL_INTERVAL_SECONDS=300
API_TIMEOUT_SECONDS=10
API_MAX_RETRIES=3
CITIES_CONFIG_PATH=config/cities.json

# --- Kafka ---
# Python on Windows talks to Kafka through the host port -> localhost:9092.
# (The Spark container overrides this to kafka:29092 in docker-compose.yml.)
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=weather-data

# --- Spark ---
# console   = print processed micro-batches (Phases 5-6, no Snowflake needed)
# snowflake = write to Snowflake (Phase 7 onwards)
SINK_MODE=console
TRIGGER_INTERVAL=1 minute
STARTING_OFFSETS=earliest

# --- Snowflake ---
# Account identifier looks like: myorg-myaccount  (NOT the full URL)
SNOWFLAKE_ACCOUNT=myorg-myaccount
SNOWFLAKE_USER=WEATHER_ETL_USER
# Password, or a Programmatic Access Token (PAT) if your account blocks passwords.
SNOWFLAKE_PASSWORD=replace_me
SNOWFLAKE_DATABASE=WEATHER_DB
SNOWFLAKE_SCHEMA=WEATHER
SNOWFLAKE_WAREHOUSE=WEATHER_WH
SNOWFLAKE_ROLE=WEATHER_ETL_ROLE
```

### `requirements.txt`

```text
# Ingestion service (runs on your Windows host inside the venv)
requests==2.32.3
confluent-kafka==2.6.1
python-dotenv==1.0.1
pytest==8.3.3
```

### `requirements-dev.txt`

```text
# OPTIONAL: only needed to run the Spark unit tests (tests/test_transformations.py)
# on your laptop. Needs Java 17 installed. Spark itself runs in Docker.
-r requirements.txt
pyspark==3.5.7
```

### `config/cities.json`

```json
{
  "cities": [
    {"name": "Delhi",     "country": "IN"},
    {"name": "Mumbai",    "country": "IN"},
    {"name": "Hyderabad", "country": "IN"},
    {"name": "Bengaluru", "country": "IN"},
    {"name": "Chennai",   "country": "IN"},
    {"name": "Kolkata",   "country": "IN"},
    {"name": "Pune",      "country": "IN"}
  ]
}
```

### `ingestion/__init__.py`

```python
"""Weather API ingestion service: OpenWeatherMap -> JSON events -> Kafka."""
```

### `ingestion/config.py`

```python
"""Loads settings from the .env file / environment. No credentials live in code."""
import json
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")


class ConfigError(Exception):
    """Raised when a required setting is missing or invalid."""


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value or value.startswith("replace"):
        raise ConfigError(f"Environment variable {name} is not set. Add it to your .env file.")
    return value


def _int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc


@dataclass(frozen=True)
class City:
    name: str
    country: str


@dataclass(frozen=True)
class Settings:
    weather_api_key: str
    weather_api_base_url: str
    weather_units: str
    poll_interval_seconds: int
    api_timeout_seconds: int
    api_max_retries: int
    kafka_bootstrap_servers: str
    kafka_topic: str
    cities: tuple


def load_cities(path: Path) -> tuple:
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        cities = tuple(City(c["name"].strip(), c.get("country", "").strip().upper()) for c in data["cities"])
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise ConfigError(f"Could not read city list from {path}: {exc}") from exc
    if not cities:
        raise ConfigError(f"City list in {path} is empty")
    return cities


def load_settings(require_api_key: bool = True) -> Settings:
    poll = _int("POLL_INTERVAL_SECONDS", 300)
    if poll < 60:
        raise ConfigError("POLL_INTERVAL_SECONDS must be >= 60 (free API tier + data only changes ~every 10 min)")

    return Settings(
        weather_api_key=_required("WEATHER_API_KEY") if require_api_key else os.getenv("WEATHER_API_KEY", ""),
        weather_api_base_url=os.getenv("WEATHER_API_BASE_URL", "https://api.openweathermap.org/data/2.5/weather"),
        weather_units=os.getenv("WEATHER_UNITS", "metric"),
        poll_interval_seconds=poll,
        api_timeout_seconds=_int("API_TIMEOUT_SECONDS", 10),
        api_max_retries=_int("API_MAX_RETRIES", 3),
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        kafka_topic=os.getenv("KAFKA_TOPIC", "weather-data"),
        cities=load_cities(Path(os.getenv("CITIES_CONFIG_PATH", "config/cities.json"))),
    )
```

### Run it

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env        # then put your WEATHER_API_KEY in .env
python -c "from ingestion.config import load_settings; s = load_settings(); print(len(s.cities), 'cities')"
```
Expected: `7 cities`

---

## Phase 2: Weather API ingestion

Guide: [docs/phase-02-weather-api-ingestion.md](phase-02-weather-api-ingestion.md)

Create the API client, the offline mock, the polling loop and its tests:

### `ingestion/weather_api.py`

```python
"""OpenWeatherMap client + conversion of an API response into a clean weather event."""
import logging
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

SCHEMA_VERSION = 1


class WeatherAPIError(Exception):
    """The API call failed (network error, bad status, non-JSON body)."""


class InvalidWeatherResponse(Exception):
    """The API answered, but the payload is missing fields we cannot do without."""


def _utc_iso(epoch_seconds):
    if epoch_seconds is None:
        return None
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).isoformat()


class WeatherClient:
    def __init__(self, api_key, base_url, units="metric", timeout=10, max_retries=3):
        self.api_key = api_key
        self.base_url = base_url
        self.units = units
        self.timeout = timeout
        self.session = requests.Session()
        # Retry transient failures (connection errors, 429 rate limit, 5xx) with
        # exponential backoff: 1s, 2s, 4s... 401/404 are NOT retried - retrying a
        # bad API key or unknown city never helps.
        retry = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.mount("http://", HTTPAdapter(max_retries=retry))

    def fetch_current(self, city, country=""):
        """Return the raw JSON dict for one city, or raise WeatherAPIError."""
        query = f"{city},{country}" if country else city
        params = {"q": query, "appid": self.api_key, "units": self.units}
        try:
            resp = self.session.get(self.base_url, params=params, timeout=self.timeout)
        except requests.RequestException as exc:
            raise WeatherAPIError(f"{city}: request failed after retries: {exc.__class__.__name__}: {exc}") from exc

        if resp.status_code == 401:
            raise WeatherAPIError(f"{city}: 401 Unauthorized - check WEATHER_API_KEY (new keys take up to 2h to activate)")
        if resp.status_code == 404:
            raise WeatherAPIError(f"{city}: 404 city not found - check config/cities.json")
        if resp.status_code != 200:
            raise WeatherAPIError(f"{city}: HTTP {resp.status_code}: {resp.text[:200]}")

        try:
            return resp.json()
        except ValueError as exc:
            raise WeatherAPIError(f"{city}: response is not valid JSON") from exc


def to_weather_event(raw, city, country, ingestion_time=None):
    """Flatten the nested OpenWeatherMap payload into our Kafka event contract.

    Required fields (no event without them): coord, dt, main.temp.
    Optional fields (rain, gust, visibility...) become None when absent.
    """
    if not isinstance(raw, dict):
        raise InvalidWeatherResponse(f"{city}: expected a JSON object, got {type(raw).__name__}")

    main = raw.get("main") or {}
    coord = raw.get("coord") or {}
    wind = raw.get("wind") or {}
    sys_ = raw.get("sys") or {}
    weather = (raw.get("weather") or [{}])[0] or {}

    missing = [name for name, value in (("coord.lat", coord.get("lat")), ("coord.lon", coord.get("lon")),
                                        ("dt", raw.get("dt")), ("main.temp", main.get("temp")))
               if value is None]
    if missing:
        raise InvalidWeatherResponse(f"{city}: response missing required field(s): {', '.join(missing)}")

    ingestion_time = ingestion_time or datetime.now(timezone.utc)
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "openweathermap",
        # We keep OUR configured city name as the business key. The API's own
        # name can differ (e.g. a locality name), so it is kept separately.
        "city": city,
        "country": sys_.get("country") or country or None,
        "city_id": raw.get("id"),
        "api_location_name": raw.get("name"),
        "latitude": coord.get("lat"),
        "longitude": coord.get("lon"),
        "api_timestamp": _utc_iso(raw.get("dt")),
        "timezone_offset_seconds": raw.get("timezone"),
        "temperature": main.get("temp"),
        "feels_like": main.get("feels_like"),
        "temp_min": main.get("temp_min"),
        "temp_max": main.get("temp_max"),
        "humidity": main.get("humidity"),
        "pressure": main.get("pressure"),
        "wind_speed": wind.get("speed"),
        "wind_direction": wind.get("deg"),
        "wind_gust": wind.get("gust"),
        "cloudiness": (raw.get("clouds") or {}).get("all"),
        "visibility": raw.get("visibility"),
        # OpenWeatherMap only sends "rain"/"snow" when it is actually raining/snowing.
        "rain_1h": (raw.get("rain") or {}).get("1h"),
        "snow_1h": (raw.get("snow") or {}).get("1h"),
        "weather_condition": weather.get("main"),
        "weather_description": weather.get("description"),
        "sunrise": _utc_iso(sys_.get("sunrise")),
        "sunset": _utc_iso(sys_.get("sunset")),
        "ingestion_timestamp": ingestion_time.isoformat(),
    }
```

### `ingestion/mock_weather.py`

```python
"""Offline stand-in for WeatherClient.

Useful while your new API key is still activating (can take ~2 hours), or for
testing Kafka/Spark without spending API calls. Returns payloads shaped exactly
like OpenWeatherMap's /data/2.5/weather response.
"""
import random
import time
import zlib

_COORDS = {
    "Delhi": (28.6667, 77.2167), "Mumbai": (19.0144, 72.8479), "Hyderabad": (17.3753, 78.4744),
    "Bengaluru": (12.9762, 77.6033), "Chennai": (13.0878, 80.2785), "Kolkata": (22.5697, 88.3697),
    "Pune": (18.5196, 73.8553),
}
_CONDITIONS = [("Clear", "clear sky"), ("Clouds", "scattered clouds"), ("Clouds", "overcast clouds"),
               ("Haze", "haze"), ("Rain", "light rain"), ("Rain", "moderate rain"), ("Thunderstorm", "thunderstorm")]


class MockWeatherClient:
    def fetch_current(self, city, country=""):
        lat, lon = _COORDS.get(city, (20.0, 78.0))
        condition, description = random.choice(_CONDITIONS)
        temp = round(random.uniform(22, 40), 2)
        now = int(time.time())
        payload = {
            "coord": {"lon": lon, "lat": lat},
            "weather": [{"id": 800, "main": condition, "description": description, "icon": "01d"}],
            "main": {"temp": temp, "feels_like": round(temp + random.uniform(-1, 5), 2),
                     "temp_min": round(temp - 1.5, 2), "temp_max": round(temp + 1.5, 2),
                     "pressure": random.randint(998, 1015), "humidity": random.randint(30, 95)},
            "visibility": random.choice([3000, 5000, 8000, 10000]),
            "wind": {"speed": round(random.uniform(0.5, 9), 2), "deg": random.randint(0, 359)},
            "clouds": {"all": random.randint(0, 100)},
            "dt": now - now % 600,  # like the real API: observation time changes ~every 10 min
            "sys": {"country": country or "IN", "sunrise": now - 20000, "sunset": now + 20000},
            "timezone": 19800,
            "id": zlib.crc32(city.encode()) % 1_000_000,
            "name": city,
            "cod": 200,
        }
        if condition in ("Rain", "Thunderstorm"):
            payload["rain"] = {"1h": round(random.uniform(0.1, 8), 2)}
        return payload
```

### `ingestion/main.py`

```python
"""Polling loop: every POLL_INTERVAL_SECONDS fetch weather for each city and publish to Kafka.

Usage (from the project root, venv activated):
    python -m ingestion.main                 # run forever
    python -m ingestion.main --once          # one cycle, then exit
    python -m ingestion.main --once --dry-run   # call the API, print events, no Kafka
    python -m ingestion.main --mock          # fake API data (no API key needed)
"""
import argparse
import json
import logging
import signal
import sys
import time

from ingestion.config import ConfigError, load_settings
from ingestion.weather_api import InvalidWeatherResponse, WeatherAPIError, WeatherClient, to_weather_event

log = logging.getLogger("ingestion")

_stop = False


def _handle_stop(signum, frame):
    global _stop
    _stop = True
    log.info("Shutdown requested - finishing current cycle...")


def run_cycle(cycle, settings, client, producer):
    started = time.monotonic()
    fetched = api_failures = invalid = published = 0
    delivered_before = producer.delivered if producer else 0
    failed_before = producer.failed if producer else 0

    for city in settings.cities:
        try:
            raw = client.fetch_current(city.name, city.country)
            fetched += 1
            event = to_weather_event(raw, city.name, city.country)
        except WeatherAPIError as exc:
            api_failures += 1
            log.error("API failure: %s", exc)
            continue
        except InvalidWeatherResponse as exc:
            invalid += 1
            log.warning("Invalid response skipped: %s", exc)
            continue

        if producer is None:  # --dry-run
            print(json.dumps(event, indent=2))
        else:
            producer.publish(event)
        published += 1
        log.info("Fetched %-10s temp=%5.1fC humidity=%s%% condition=%s",
                 city.name, event["temperature"], event["humidity"], event["weather_condition"])

    pending = producer.flush(30) if producer else 0
    delivered = (producer.delivered - delivered_before) if producer else 0
    delivery_failed = (producer.failed - failed_before) if producer else 0
    log.info("CYCLE %d SUMMARY cities=%d fetched=%d api_failures=%d invalid=%d published=%d "
             "delivered=%d delivery_failed=%d pending=%d duration=%.1fs",
             cycle, len(settings.cities), fetched, api_failures, invalid, published,
             delivered, delivery_failed, pending, time.monotonic() - started)


def main():
    parser = argparse.ArgumentParser(description="Weather API -> Kafka ingestion service")
    parser.add_argument("--once", action="store_true", help="run a single polling cycle and exit")
    parser.add_argument("--dry-run", action="store_true", help="print events instead of sending to Kafka")
    parser.add_argument("--mock", action="store_true", help="use fake weather data instead of the real API")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level.upper(),
                        format="%(asctime)s %(levelname)-7s %(name)s - %(message)s")

    try:
        settings = load_settings(require_api_key=not args.mock)
    except ConfigError as exc:
        log.error("Configuration error: %s", exc)
        return 2

    if args.mock:
        from ingestion.mock_weather import MockWeatherClient
        client = MockWeatherClient()
        log.warning("MOCK MODE: using generated weather data, not the real API")
    else:
        client = WeatherClient(settings.weather_api_key, settings.weather_api_base_url, settings.weather_units,
                               settings.api_timeout_seconds, settings.api_max_retries)

    producer = None
    if not args.dry_run:
        from ingestion.kafka_producer import WeatherProducer  # imported lazily so --dry-run works without Kafka
        producer = WeatherProducer(settings.kafka_bootstrap_servers, settings.kafka_topic)
        try:
            producer.check_connection()
        except RuntimeError as exc:
            log.error("%s", exc)
            return 1
        log.info("Connected to Kafka at %s, topic=%s", settings.kafka_bootstrap_servers, settings.kafka_topic)

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)
    log.info("Polling %d cities every %ds", len(settings.cities), settings.poll_interval_seconds)

    cycle = 0
    while not _stop:
        cycle += 1
        started = time.monotonic()
        run_cycle(cycle, settings, client, producer)
        if args.once:
            break
        # Sleep in 1s steps so Ctrl+C is handled quickly.
        while not _stop and time.monotonic() - started < settings.poll_interval_seconds:
            time.sleep(1)

    if producer:
        producer.flush(30)
    log.info("Ingestion stopped. Total delivered=%d failed=%d",
             producer.delivered if producer else 0, producer.failed if producer else 0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

### `tests/test_weather_api.py`

```python
"""Unit tests for the ingestion event mapping (no network, no Kafka).   Run: pytest -q"""
import pytest

from ingestion.mock_weather import MockWeatherClient
from ingestion.weather_api import InvalidWeatherResponse, to_weather_event

SAMPLE = {
    "coord": {"lon": 77.2167, "lat": 28.6667},
    "weather": [{"id": 721, "main": "Haze", "description": "haze", "icon": "50d"}],
    "main": {"temp": 31.05, "feels_like": 35.2, "temp_min": 31.05, "temp_max": 31.05,
             "pressure": 1004, "humidity": 62},
    "visibility": 3000,
    "wind": {"speed": 2.57, "deg": 90},
    "clouds": {"all": 20},
    "dt": 1759049400,
    "sys": {"country": "IN", "sunrise": 1759020000, "sunset": 1759063000},
    "timezone": 19800, "id": 1273294, "name": "Delhi", "cod": 200,
}


def test_maps_all_fields():
    event = to_weather_event(SAMPLE, "Delhi", "IN")
    assert event["city"] == "Delhi"
    assert event["country"] == "IN"
    assert event["temperature"] == 31.05
    assert event["humidity"] == 62
    assert event["weather_condition"] == "Haze"
    assert event["api_timestamp"] == "2025-09-28T08:50:00+00:00"
    assert event["rain_1h"] is None           # no "rain" block -> None (Spark turns it into 0)
    assert event["ingestion_timestamp"]


def test_rain_is_extracted_when_present():
    event = to_weather_event({**SAMPLE, "rain": {"1h": 2.4}}, "Delhi", "IN")
    assert event["rain_1h"] == 2.4


@pytest.mark.parametrize("broken", [
    {k: v for k, v in SAMPLE.items() if k != "dt"},
    {**SAMPLE, "main": {}},
    {**SAMPLE, "coord": None},
])
def test_missing_required_fields_raise(broken):
    with pytest.raises(InvalidWeatherResponse):
        to_weather_event(broken, "Delhi", "IN")


def test_missing_optional_blocks_become_none():
    minimal = {"coord": {"lat": 1, "lon": 2}, "dt": 1759049400, "main": {"temp": 20}}
    event = to_weather_event(minimal, "Pune", "IN")
    assert event["weather_condition"] is None and event["wind_speed"] is None
    assert event["country"] == "IN"


def test_non_dict_response_raises():
    with pytest.raises(InvalidWeatherResponse):
        to_weather_event(["not", "a", "dict"], "Delhi", "IN")


def test_mock_client_produces_valid_events():
    event = to_weather_event(MockWeatherClient().fetch_current("Mumbai", "IN"), "Mumbai", "IN")
    assert event["city"] == "Mumbai" and event["temperature"] is not None
```

### Run it

```powershell
python -m ingestion.main --once --dry-run          # real API, prints events
python -m ingestion.main --once --dry-run --mock   # no API key needed
pytest -q tests/test_weather_api.py                # 8 passed
```

---

## Phase 3: Kafka producer

Guide: [docs/phase-03-kafka-producer.md](phase-03-kafka-producer.md)

Create the Docker Compose file and the Kafka producer:

### `docker/docker-compose.yml`

```yaml
# Local infrastructure for the Weather ETL POC.
#
#   docker compose up -d                    -> Kafka + Kafka UI   (Phases 1-4)
#   docker compose --profile spark up spark -> + Spark streaming job (Phase 5+)
#
# Run these commands from the docker/ folder.

services:
  kafka:
    # Single Kafka broker in KRaft mode (no ZooKeeper needed).
    image: apache/kafka:3.9.1
    container_name: weather-kafka
    ports:
      - "9092:9092"            # for apps on your laptop (Python producer)
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      # Three listeners:
      #   HOST       -> localhost:9092 (Python on Windows)
      #   DOCKER     -> kafka:29092    (Kafka UI + Spark containers)
      #   CONTROLLER -> internal KRaft traffic
      KAFKA_LISTENERS: HOST://0.0.0.0:9092,DOCKER://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: HOST://localhost:9092,DOCKER://kafka:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: HOST:PLAINTEXT,DOCKER:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: DOCKER
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      # Single broker -> every replication factor must be 1.
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      # We create topics explicitly, so typos don't silently create new topics.
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      # Keep 7 days of weather events (enough to replay into Spark if needed).
      KAFKA_LOG_RETENTION_HOURS: 168
      KAFKA_LOG_DIRS: /var/lib/kafka/data
    volumes:
      - kafka-data:/var/lib/kafka/data
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1"]
      interval: 10s
      timeout: 10s
      retries: 10

  kafka-ui:
    image: kafbat/kafka-ui:v1.3.0
    container_name: weather-kafka-ui
    ports:
      - "8080:8080"            # open http://localhost:8080
    environment:
      KAFKA_CLUSTERS_0_NAME: weather-local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
    depends_on:
      kafka:
        condition: service_healthy

  spark:
    # Spark runs in a Linux container so you avoid winutils.exe / HADOOP_HOME
    # problems that PySpark streaming has on native Windows.
    image: apache/spark:3.5.7-java17-python3
    container_name: weather-spark
    profiles: ["spark"]        # only starts when you ask for it
    user: root                 # avoids file-permission issues on mounted folders
    depends_on:
      kafka:
        condition: service_healthy
    env_file:
      - ../.env                # same .env the Python producer uses
    environment:
      # Inside Docker, Kafka is reached by its container name, not localhost.
      KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      CHECKPOINT_ROOT: /app/checkpoints
      PYTHONUNBUFFERED: "1"
    volumes:
      - ../spark:/app/spark:ro
      - ../checkpoints:/app/checkpoints
      - spark-ivy:/root/.ivy2  # caches downloaded connector jars between runs
    ports:
      - "4040:4040"            # Spark UI while the job is running
    command: >
      /opt/spark/bin/spark-submit
      --master local[2]
      --conf spark.jars.ivy=/root/.ivy2
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,net.snowflake:spark-snowflake_2.12:3.1.9
      /app/spark/weather_stream.py

volumes:
  kafka-data:
  spark-ivy:
```

### `ingestion/kafka_producer.py`

```python
"""Thin wrapper around confluent-kafka's Producer with delivery tracking."""
import json
import logging

from confluent_kafka import KafkaException, Producer

log = logging.getLogger(__name__)


class WeatherProducer:
    def __init__(self, bootstrap_servers, topic):
        self.topic = topic
        self.delivered = 0
        self.failed = 0
        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "client.id": "weather-ingestion",
            "acks": "all",                 # wait until the broker has stored the message
            "enable.idempotence": True,    # broker drops duplicates caused by producer retries
            "linger.ms": 50,               # tiny batching window; we send 7 msgs per cycle
            "message.timeout.ms": 30000,   # give up on a message after 30s
        })

    def check_connection(self, timeout=10):
        """Fail fast at startup if Kafka is down or the topic doesn't exist."""
        try:
            metadata = self.producer.list_topics(timeout=timeout)
        except KafkaException as exc:
            raise RuntimeError(f"Cannot reach Kafka: {exc}. Is 'docker compose up -d' running?") from exc
        if self.topic not in metadata.topics:
            raise RuntimeError(f"Kafka topic '{self.topic}' does not exist. Create it first (see Phase 4).")

    def _on_delivery(self, err, msg):
        # Called from poll()/flush() once the broker confirms (or rejects) a message.
        if err is not None:
            self.failed += 1
            log.error("Delivery failed key=%s: %s", msg.key(), err)
        else:
            self.delivered += 1
            log.debug("Delivered key=%s partition=%s offset=%s", msg.key(), msg.partition(), msg.offset())

    def publish(self, event):
        # Key = city, so all events of one city land on the same partition (ordered per city).
        self.producer.produce(
            self.topic,
            key=event["city"].encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            on_delivery=self._on_delivery,
        )
        self.producer.poll(0)  # serve delivery callbacks for earlier messages

    def flush(self, timeout=30):
        """Block until all queued messages are delivered; returns how many are still pending."""
        return self.producer.flush(timeout)
```

### Run it

```powershell
cd docker
docker compose up -d
docker exec weather-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic weather-data --partitions 3 --replication-factor 1
cd ..
python -m ingestion.main --once        # CYCLE 1 SUMMARY ... delivered=7
```

---

## Phase 4: Kafka topic and consumer validation

Guide: [docs/phase-04-kafka-topic-consumer-validation.md](phase-04-kafka-topic-consumer-validation.md)

Create the validation consumer:

### `scripts/consume_test.py`

```python
"""Phase 4 validation: read messages from the Kafka topic and print them.

    python scripts/consume_test.py                 # read everything from the beginning, stop after 10s idle
    python scripts/consume_test.py --follow        # keep waiting for new messages (Ctrl+C to stop)

Uses its own consumer group ("weather-validator"), so it never interferes with Spark
(Spark tracks its offsets in its checkpoint, not in a consumer group).
"""
import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path

from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--follow", action="store_true", help="keep consuming until Ctrl+C")
    parser.add_argument("--group", default=None, help="consumer group id (default: a fresh one each run)")
    args = parser.parse_args()

    # A fresh group id each run + auto.offset.reset=earliest -> always re-reads the whole topic.
    group = args.group or f"weather-validator-{uuid.uuid4().hex[:6]}"
    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    topic = os.getenv("KAFKA_TOPIC", "weather-data")
    consumer.subscribe([topic])
    print(f"Consuming '{topic}' as group '{group}'...")

    count, per_city, last_msg = 0, {}, time.monotonic()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if not args.follow and time.monotonic() - last_msg > 10:
                    break
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"ERROR: {msg.error()}", file=sys.stderr)
                continue
            last_msg = time.monotonic()
            count += 1
            key = msg.key().decode() if msg.key() else None
            try:
                event = json.loads(msg.value())
                summary = f"{event.get('city')}: {event.get('temperature')}C {event.get('weather_condition')} @ {event.get('api_timestamp')}"
            except ValueError:
                summary = f"<not JSON> {msg.value()[:60]!r}"
            per_city[key] = per_city.get(key, 0) + 1
            print(f"partition={msg.partition()} offset={msg.offset():<5} key={key:<10} {summary}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    print(f"\nConsumed {count} messages. Per key: {dict(sorted(per_city.items(), key=lambda kv: str(kv[0])))}")


if __name__ == "__main__":
    main()
```

### Run it

```powershell
python scripts/consume_test.py         # Consumed 7 messages. Per key: {...}
```
Kafka UI: http://localhost:8080

---

## Phase 5: Spark Structured Streaming

Guide: [docs/phase-05-spark-structured-streaming.md](phase-05-spark-structured-streaming.md)

The Spark job imports all four modules below, so create them all now. Phase 6 explains `transformations.py`, and Phase 8 explains `data_quality.py`.

### `spark/schemas.py`

```python
"""Explicit schema of the JSON event produced by ingestion/weather_api.py.

Why explicit? Streaming sources cannot infer schemas, and an explicit contract
means a producer change (renamed/removed field) shows up as NULLs that our data
quality checks catch - instead of silently changing the table structure.

Numbers are read as DoubleType and timestamps as strings on purpose: a humidity
of 65.0 or a timestamp in an unexpected format should not make the whole
record unreadable. We cast to the final types in transformations.clean().
"""
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

WEATHER_EVENT_SCHEMA = StructType([
    StructField("schema_version", IntegerType()),
    StructField("source", StringType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("city_id", LongType()),
    StructField("api_location_name", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("api_timestamp", StringType()),
    StructField("timezone_offset_seconds", DoubleType()),
    StructField("temperature", DoubleType()),
    StructField("feels_like", DoubleType()),
    StructField("temp_min", DoubleType()),
    StructField("temp_max", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("pressure", DoubleType()),
    StructField("wind_speed", DoubleType()),
    StructField("wind_direction", DoubleType()),
    StructField("wind_gust", DoubleType()),
    StructField("cloudiness", DoubleType()),
    StructField("visibility", DoubleType()),
    StructField("rain_1h", DoubleType()),
    StructField("snow_1h", DoubleType()),
    StructField("weather_condition", StringType()),
    StructField("weather_description", StringType()),
    StructField("sunrise", StringType()),
    StructField("sunset", StringType()),
    StructField("ingestion_timestamp", StringType()),
])
```

### `spark/transformations.py`

```python
"""Pure DataFrame transformations: Kafka rows -> parsed -> cleaned -> enriched.

Every function takes a DataFrame and returns a DataFrame, and none of them use
Python UDFs, so all work runs inside Spark's JVM engine (fast) and each step
can be unit-tested with a normal batch DataFrame (see tests/).
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from schemas import WEATHER_EVENT_SCHEMA

RAIN_CONDITIONS = ["Rain", "Drizzle", "Thunderstorm"]


def parse_kafka_messages(kafka_df: DataFrame) -> DataFrame:
    """Kafka gives us binary key/value + metadata. Decode and parse the JSON."""
    return (
        kafka_df
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_payload"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        # get_json_object returns NULL when the payload is not valid JSON at all.
        # from_json alone can't tell "malformed" apart from "valid JSON with nulls".
        .withColumn("is_valid_json", F.get_json_object("raw_payload", "$").isNotNull()
                    & F.trim("raw_payload").startswith("{"))
        .withColumn("data", F.from_json("raw_payload", WEATHER_EVENT_SCHEMA))
        .select("*", "data.*")
        .drop("data")
    )


def clean(df: DataFrame) -> DataFrame:
    """Standardise text, cast to final types, and handle NULLs explicitly."""
    return (
        df
        # Text: trim whitespace, consistent casing (" delhi " -> "Delhi", "in" -> "IN")
        .withColumn("city", F.initcap(F.trim("city")))
        .withColumn("country", F.upper(F.trim("country")))
        .withColumn("weather_condition", F.initcap(F.trim("weather_condition")))
        .withColumn("weather_description", F.lower(F.trim("weather_description")))
        # Empty strings are as useless as NULL -> make them NULL so DQ checks catch them
        .withColumn("city", F.when(F.col("city") != "", F.col("city")))
        .withColumn("weather_condition", F.when(F.col("weather_condition") != "", F.col("weather_condition")))
        # ISO-8601 strings -> real timestamps (session timezone is UTC)
        .withColumn("api_timestamp", F.to_timestamp("api_timestamp"))
        .withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp"))
        .withColumn("sunrise", F.to_timestamp("sunrise"))
        .withColumn("sunset", F.to_timestamp("sunset"))
        # Whole-number measures -> integers
        .withColumn("humidity", F.round("humidity").cast("int"))
        .withColumn("pressure", F.round("pressure").cast("int"))
        .withColumn("cloudiness", F.round("cloudiness").cast("int"))
        .withColumn("visibility", F.round("visibility").cast("int"))
        .withColumn("wind_direction", F.round("wind_direction").cast("int"))
        .withColumn("timezone_offset_seconds", F.coalesce(F.col("timezone_offset_seconds").cast("int"), F.lit(0)))
        # The API omits "rain"/"snow" when it isn't raining/snowing -> that means 0 mm, not unknown.
        .withColumn("rain_1h", F.coalesce("rain_1h", F.lit(0.0)))
        .withColumn("snow_1h", F.coalesce("snow_1h", F.lit(0.0)))
        # Decimal measures -> 2 decimal places
        .withColumn("temperature", F.round("temperature", 2))
        .withColumn("feels_like", F.round("feels_like", 2))
        .withColumn("temp_min", F.round("temp_min", 2))
        .withColumn("temp_max", F.round("temp_max", 2))
        .withColumn("wind_speed", F.round("wind_speed", 2))
        .withColumn("latitude", F.round("latitude", 4))
        .withColumn("longitude", F.round("longitude", 4))
    )


def _heat_index_c(t_c, rh):
    """NOAA heat index (Rothfusz regression). Computed in Fahrenheit, returned in Celsius.
    Only meaningful when hot (>= 27C / 80F) and humid (>= 40%); otherwise we use the air temperature."""
    t = t_c * 9 / 5 + 32
    hi_f = (-42.379 + 2.04901523 * t + 10.14333127 * rh - 0.22475541 * t * rh
            - 0.00683783 * t * t - 0.05481717 * rh * rh + 0.00122874 * t * t * rh
            + 0.00085282 * t * rh * rh - 0.00000199 * t * t * rh * rh)
    return F.when((t_c >= 27) & (rh >= 40), (hi_f - 32) * 5 / 9).otherwise(t_c)


def add_derived_fields(df: DataFrame) -> DataFrame:
    temp, hum, wind = F.col("temperature"), F.col("humidity"), F.col("wind_speed")
    return (
        df
        # Deterministic business key: same city + same API observation time = same event.
        # OpenWeatherMap only refreshes ~every 10 min, so polling every 5 min WILL produce
        # repeats; this key lets us drop them.
        .withColumn("event_id", F.sha2(F.concat_ws("|", F.upper("city"), F.date_format(
            "api_timestamp", "yyyy-MM-dd'T'HH:mm:ss")), 256))
        # Local time of the observation (IST for India) - what dashboard users expect.
        .withColumn("observation_ts_local",
                    F.timestamp_seconds(F.unix_timestamp("api_timestamp") + F.col("timezone_offset_seconds")))
        .withColumn("observation_date", F.to_date("observation_ts_local"))
        .withColumn("observation_hour", F.hour("observation_ts_local"))
        .withColumn("temperature_category",
                    F.when(temp.isNull(), None).when(temp < 10, "Cold").when(temp < 20, "Cool").when(temp < 30, "Moderate")
                     .when(temp < 38, "Hot").otherwise("Extreme Heat"))
        # Simplified Beaufort scale (m/s)
        .withColumn("wind_category",
                    F.when(wind.isNull(), "Unknown").when(wind < 0.5, "Calm").when(wind < 5.5, "Light")
                     .when(wind < 10.8, "Moderate").when(wind < 17.2, "Strong").otherwise("Gale"))
        .withColumn("is_rainy", (F.col("rain_1h") > 0) | F.col("weather_condition").isin(RAIN_CONDITIONS))
        .withColumn("heat_index", F.round(_heat_index_c(temp, hum), 2))
        # NOAA heat-index bands converted to Celsius
        .withColumn("heat_index_category",
                    F.when(F.col("heat_index").isNull(), None).when(F.col("heat_index") < 27, "Normal").when(F.col("heat_index") < 32, "Caution")
                     .when(F.col("heat_index") < 41, "Extreme Caution").when(F.col("heat_index") < 54, "Danger")
                     .otherwise("Extreme Danger"))
        .withColumn("processing_timestamp", F.current_timestamp())
    )
```

### `spark/data_quality.py`

```python
"""Rule-based data quality checks.

Every record gets a `dq_errors` array. Empty array -> valid. Non-empty -> the
record is routed to the rejected table WITH the reasons, so nothing silently
disappears and you can query why a record was rejected.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def dq_rules():
    """(rule name, condition that makes the record BAD). A function, not a constant,
    because Column expressions can only be built once a SparkSession exists."""
    return [
        ("NULL_CITY", F.col("city").isNull()),
        ("NULL_OR_BAD_API_TIMESTAMP", F.col("api_timestamp").isNull()),
        ("INVALID_TEMPERATURE", F.col("temperature").isNull() | ~F.col("temperature").between(-60, 60)),
        ("INVALID_HUMIDITY", F.col("humidity").isNull() | ~F.col("humidity").between(0, 100)),
        ("MISSING_WEATHER_CONDITION", F.col("weather_condition").isNull()),
        ("INVALID_LATITUDE", F.col("latitude").isNull() | ~F.col("latitude").between(-90, 90)),
        ("INVALID_LONGITUDE", F.col("longitude").isNull() | ~F.col("longitude").between(-180, 180)),
        ("INVALID_PRESSURE", F.col("pressure").isNotNull() & ~F.col("pressure").between(850, 1100)),
        ("NEGATIVE_WIND_SPEED", F.col("wind_speed") < 0),
    ]


def apply_quality_checks(df: DataFrame) -> DataFrame:
    """Adds dq_errors (array<string>), dq_reason (string) and is_valid (boolean)."""
    rule_checks = F.array(*[F.when(condition, F.lit(name)) for name, condition in dq_rules()])
    errors = F.when(~F.col("is_valid_json"), F.array(F.lit("MALFORMED_JSON"))) \
              .otherwise(F.filter(rule_checks, lambda x: x.isNotNull()))
    return (
        df.withColumn("dq_errors", errors)
          .withColumn("dq_reason", F.array_join("dq_errors", ","))
          .withColumn("is_valid", F.size("dq_errors") == 0)
    )


def split_valid_rejected(checked_df: DataFrame):
    """Returns (valid_df, rejected_df). valid_df is de-duplicated on event_id within the batch."""
    valid = checked_df.filter("is_valid").dropDuplicates(["event_id"])
    rejected = checked_df.filter(~F.col("is_valid"))
    return valid, rejected
```

### `spark/weather_stream.py`

```python
"""Spark Structured Streaming job: Kafka -> parse -> clean -> enrich -> DQ -> sink.

Run it in Docker (recommended on Windows):
    cd docker
    docker compose --profile spark up spark

Settings come from environment variables (the root .env via docker-compose):
    SINK_MODE          console | snowflake
    TRIGGER_INTERVAL   e.g. "1 minute"
    STARTING_OFFSETS   earliest | latest (only used on the very first start)
"""
import logging
import os
import sys
import time

from pyspark.sql import SparkSession

from data_quality import apply_quality_checks, split_valid_rejected
from transformations import add_derived_fields, clean, parse_kafka_messages

logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                    format="%(asctime)s %(levelname)-7s %(name)s - %(message)s")
log = logging.getLogger("weather_stream")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather-data")
SINK_MODE = os.getenv("SINK_MODE", "console").strip().lower()
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "1 minute")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")
# Separate checkpoint per sink: switching console -> snowflake re-reads the topic from the start.
CHECKPOINT_DIR = os.path.join(os.getenv("CHECKPOINT_ROOT", "./checkpoints"), f"weather_{SINK_MODE}")


def build_spark():
    spark = (SparkSession.builder
             .appName("weather-streaming-etl")
             .config("spark.sql.session.timeZone", "UTC")    # all timestamps handled in UTC
             .config("spark.sql.shuffle.partitions", "2")    # tiny data: default 200 is wasteful
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")                  # keep our own logs readable
    return spark


def transform(kafka_batch_df):
    """The full pipeline for one micro-batch, as a plain DataFrame -> DataFrame chain."""
    parsed = parse_kafka_messages(kafka_batch_df)
    enriched = add_derived_fields(clean(parsed))
    return apply_quality_checks(enriched)


def make_batch_processor(spark, sf_options):
    def process_batch(kafka_batch_df, batch_id):
        started = time.monotonic()
        checked = transform(kafka_batch_df).persist()   # reused by several counts/writes below
        try:
            valid, rejected = split_valid_rejected(checked)
            total = checked.count()
            valid_before_dedup = checked.filter("is_valid").count()
            valid_count = valid.count()
            rejected_count = rejected.count()

            if rejected_count:
                for row in rejected.select("kafka_partition", "kafka_offset", "city", "dq_reason").collect():
                    log.warning("REJECTED batch=%s partition=%s offset=%s city=%s reasons=%s",
                                batch_id, row.kafka_partition, row.kafka_offset, row.city, row.dq_reason)

            written = {}
            if SINK_MODE == "snowflake":
                from snowflake_writer import write_batch_to_snowflake
                written = write_batch_to_snowflake(spark, checked, valid, rejected, batch_id, sf_options)
            else:
                (valid.select("city", "observation_ts_local", "temperature", "humidity", "wind_speed",
                              "weather_condition", "temperature_category", "wind_category", "is_rainy",
                              "heat_index_category")
                      .orderBy("city").show(truncate=False))
                if rejected_count:
                    rejected.select("kafka_offset", "city", "dq_reason", "raw_payload").show(truncate=60)

            log.info("BATCH %s SUMMARY consumed=%d valid=%d rejected=%d in_batch_duplicates=%d%s duration=%.1fs",
                     batch_id, total, valid_count, rejected_count, valid_before_dedup - valid_count,
                     "".join(f" {k}={v}" for k, v in written.items()), time.monotonic() - started)
        finally:
            checked.unpersist()
    return process_batch


def main():
    if SINK_MODE not in ("console", "snowflake"):
        log.error("SINK_MODE must be 'console' or 'snowflake', got %r", SINK_MODE)
        return 2

    sf_options = None
    if SINK_MODE == "snowflake":
        from snowflake_writer import snowflake_options
        sf_options = snowflake_options()   # fail fast if .env is incomplete

    spark = build_spark()
    log.info("Reading topic=%s from %s | sink=%s | trigger=%s | checkpoint=%s",
             KAFKA_TOPIC, KAFKA_BOOTSTRAP, SINK_MODE, TRIGGER_INTERVAL, CHECKPOINT_DIR)

    kafka_stream = (spark.readStream.format("kafka")
                    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                    .option("subscribe", KAFKA_TOPIC)
                    .option("startingOffsets", STARTING_OFFSETS)
                    .option("maxOffsetsPerTrigger", 5000)   # cap batch size when catching up
                    .option("failOnDataLoss", "false")      # don't die if old offsets were deleted by retention
                    .load())

    query = (kafka_stream.writeStream
             .queryName("weather_etl")
             .foreachBatch(make_batch_processor(spark, sf_options))
             .option("checkpointLocation", CHECKPOINT_DIR)
             .trigger(processingTime=TRIGGER_INTERVAL)
             .start())

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        log.info("Stopping stream...")
        query.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

### Run it

Make sure `.env` has `SINK_MODE=console`, then:
```powershell
cd docker
docker compose --profile spark up spark
# in another terminal, from the project root:
python -m ingestion.main --mock
```
Expected: a table of cities and `BATCH n SUMMARY consumed=7 valid=7 ...`

---

## Phase 6: Data cleaning and transformation

Guide: [docs/phase-06-cleaning-transformation.md](phase-06-cleaning-transformation.md)

The code (`spark/transformations.py`) was created in Phase 5. Add its unit tests (optional; they need Java 17):

### `tests/test_transformations.py`

```python
"""Tests the Spark transformation + DQ logic on a normal (batch) DataFrame that
looks exactly like what the Kafka source produces. Needs Java 17 + pyspark.

    pytest -q tests/test_transformations.py
"""
import json
import sys
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "spark"))

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from data_quality import apply_quality_checks, split_valid_rejected  # noqa: E402
from transformations import add_derived_fields, clean, parse_kafka_messages  # noqa: E402

GOOD = {
    "city": " delhi ", "country": "in", "latitude": 28.6667, "longitude": 77.2167,
    "api_timestamp": "2025-09-28T08:50:00+00:00", "timezone_offset_seconds": 19800,
    "temperature": 34.0, "feels_like": 38.0, "temp_min": 33.0, "temp_max": 35.0,
    "humidity": 60, "pressure": 1004, "wind_speed": 3.2, "wind_direction": 90,
    "cloudiness": 20, "visibility": 3000, "weather_condition": "haze", "weather_description": "Haze",
    "ingestion_timestamp": "2025-09-28T08:52:01+00:00",
}


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[1]").appName("tests")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "1").getOrCreate())
    yield session
    session.stop()


def kafka_df(spark, payloads):
    rows = [(p["city"].strip().encode() if isinstance(p, dict) and p.get("city") else None,
             (p if isinstance(p, str) else json.dumps(p)).encode(),
             "weather-data", 0, i, datetime(2025, 9, 28, 8, 52))
            for i, p in enumerate(payloads)]
    return spark.createDataFrame(rows, "key binary, value binary, topic string, partition int, offset long, timestamp timestamp")


def run(spark, payloads):
    checked = apply_quality_checks(add_derived_fields(clean(parse_kafka_messages(kafka_df(spark, payloads)))))
    return {r.kafka_offset: r for r in checked.collect()}, checked


def test_valid_record_is_cleaned_and_enriched(spark):
    rows, _ = run(spark, [GOOD])
    r = rows[0]
    assert r.is_valid and r.dq_reason == ""
    assert r.city == "Delhi" and r.country == "IN"
    assert r.weather_condition == "Haze" and r.weather_description == "haze"
    assert r.observation_ts_local == datetime(2025, 9, 28, 14, 20)   # UTC + 5:30
    assert str(r.observation_date) == "2025-09-28" and r.observation_hour == 14
    assert r.temperature_category == "Hot"
    assert r.wind_category == "Light"
    assert r.rain_1h == 0.0 and r.is_rainy is False
    assert r.heat_index > r.temperature and r.heat_index_category == "Danger"
    assert len(r.event_id) == 64


@pytest.mark.parametrize("payload, expected", [
    ("not json at all {", "MALFORMED_JSON"),
    ({**GOOD, "city": None}, "NULL_CITY"),
    ({**GOOD, "city": "   "}, "NULL_CITY"),
    ({**GOOD, "api_timestamp": "garbage"}, "NULL_OR_BAD_API_TIMESTAMP"),
    ({**GOOD, "temperature": 99}, "INVALID_TEMPERATURE"),
    ({**GOOD, "temperature": "hot"}, "INVALID_TEMPERATURE"),
    ({**GOOD, "humidity": 150}, "INVALID_HUMIDITY"),
    ({**GOOD, "weather_condition": None}, "MISSING_WEATHER_CONDITION"),
    ({**GOOD, "latitude": 95}, "INVALID_LATITUDE"),
    ({**GOOD, "longitude": -200}, "INVALID_LONGITUDE"),
])
def test_bad_records_are_rejected_with_reason(spark, payload, expected):
    rows, _ = run(spark, [payload])
    r = rows[0]
    assert r.is_valid is False
    assert expected in r.dq_reason.split(",")


def test_multiple_reasons_are_all_reported(spark):
    rows, _ = run(spark, [{**GOOD, "humidity": -5, "latitude": 500}])
    assert set(rows[0].dq_reason.split(",")) == {"INVALID_HUMIDITY", "INVALID_LATITUDE"}


def test_duplicates_in_a_batch_are_dropped(spark):
    # Same city (different spelling/whitespace) + same API timestamp = same observation.
    _, checked = run(spark, [GOOD, {**GOOD, "city": "Delhi"}, {**GOOD, "city": "Mumbai"}])
    valid, rejected = split_valid_rejected(checked)
    assert valid.count() == 2 and rejected.count() == 0


def test_rain_flag(spark):
    rows, _ = run(spark, [{**GOOD, "rain_1h": 1.2}, {**GOOD, "city": "Pune", "weather_condition": "Drizzle"}])
    assert rows[0].is_rainy and rows[1].is_rainy


def test_float_humidity_is_accepted(spark):
    rows, _ = run(spark, [{**GOOD, "humidity": 65.0}])
    assert rows[0].is_valid and rows[0].humidity == 65
```

### Run it

```powershell
pip install -r requirements-dev.txt
pytest -q tests/test_transformations.py     # 15 passed
```

---

## Phase 7: Snowflake integration

Guide: [docs/phase-07-snowflake-integration.md](phase-07-snowflake-integration.md)

Run the three SQL files in a Snowsight worksheet, in order. Create the Spark → Snowflake writer:

### `snowflake/01_setup.sql`

```sql
-- =====================================================================
-- 01_setup.sql  -  Warehouse, database, schema, role and service user
-- Run as ACCOUNTADMIN (a trial account gives you this) in a Snowsight worksheet.
-- =====================================================================
USE ROLE ACCOUNTADMIN;

-- X-Small is plenty: we load a handful of rows per micro-batch.
-- AUTO_SUSPEND = 60 stops credit burn as soon as the pipeline goes quiet.
CREATE WAREHOUSE IF NOT EXISTS WEATHER_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Weather ETL POC - Spark loads + Power BI queries';

CREATE DATABASE IF NOT EXISTS WEATHER_DB COMMENT = 'Weather ETL POC';
CREATE SCHEMA IF NOT EXISTS WEATHER_DB.WEATHER COMMENT = 'Raw, fact, dimension and reporting views';

-- One role for the pipeline (writes) and a read-only role for Power BI.
CREATE ROLE IF NOT EXISTS WEATHER_ETL_ROLE;
CREATE ROLE IF NOT EXISTS WEATHER_BI_ROLE;

GRANT USAGE ON WAREHOUSE WEATHER_WH TO ROLE WEATHER_ETL_ROLE;
GRANT USAGE ON WAREHOUSE WEATHER_WH TO ROLE WEATHER_BI_ROLE;
GRANT USAGE ON DATABASE WEATHER_DB TO ROLE WEATHER_ETL_ROLE;
GRANT USAGE ON DATABASE WEATHER_DB TO ROLE WEATHER_BI_ROLE;
GRANT ALL ON SCHEMA WEATHER_DB.WEATHER TO ROLE WEATHER_ETL_ROLE;   -- needs CREATE STAGE for the Spark connector
GRANT USAGE ON SCHEMA WEATHER_DB.WEATHER TO ROLE WEATHER_BI_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA WEATHER_DB.WEATHER TO ROLE WEATHER_BI_ROLE;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA WEATHER_DB.WEATHER TO ROLE WEATHER_BI_ROLE;
GRANT ROLE WEATHER_ETL_ROLE TO ROLE SYSADMIN;
GRANT ROLE WEATHER_BI_ROLE  TO ROLE SYSADMIN;

-- Service user for Spark. Replace the password (or see the PAT option below).
-- TYPE = LEGACY_SERVICE allows password login for non-human users on accounts
-- where that is still permitted. Never put this password in code - only in .env.
CREATE USER IF NOT EXISTS WEATHER_ETL_USER
  PASSWORD = 'Change-Me-Str0ng-Passw0rd!'
  TYPE = LEGACY_SERVICE
  DEFAULT_ROLE = WEATHER_ETL_ROLE
  DEFAULT_WAREHOUSE = WEATHER_WH
  DEFAULT_NAMESPACE = WEATHER_DB.WEATHER
  COMMENT = 'Spark Structured Streaming loader';
GRANT ROLE WEATHER_ETL_ROLE TO USER WEATHER_ETL_USER;

-- Let YOUR login use the BI role from Power BI (replace with your username).
-- GRANT ROLE WEATHER_BI_ROLE TO USER <YOUR_SNOWFLAKE_LOGIN>;

-- ---------------------------------------------------------------------
-- OPTION: if your account rejects password logins, create a Programmatic
-- Access Token and put the token string in SNOWFLAKE_PASSWORD in .env.
-- (PATs require a network policy unless an authentication policy relaxes it;
--  see Snowflake docs "Using programmatic access tokens".)
-- ALTER USER WEATHER_ETL_USER SET TYPE = SERVICE;
-- ALTER USER WEATHER_ETL_USER ADD PROGRAMMATIC ACCESS TOKEN WEATHER_ETL_PAT
--   ROLE_RESTRICTION = 'WEATHER_ETL_ROLE' DAYS_TO_EXPIRY = 90;
-- ---------------------------------------------------------------------

-- Optional safety net for a trial account: cap spend at 5 credits / month.
CREATE RESOURCE MONITOR IF NOT EXISTS WEATHER_POC_MONITOR
  WITH CREDIT_QUOTA = 5 FREQUENCY = MONTHLY START_TIMESTAMP = IMMEDIATELY
  TRIGGERS ON 80 PERCENT DO NOTIFY
           ON 100 PERCENT DO SUSPEND;
ALTER WAREHOUSE WEATHER_WH SET RESOURCE_MONITOR = WEATHER_POC_MONITOR;
```

### `snowflake/02_tables.sql`

```sql
-- =====================================================================
-- 02_tables.sql  -  Tables
--
--   Kafka ──> RAW_WEATHER        every message exactly as received (audit / replay)
--        ├──> WEATHER_REJECTED   messages that failed data quality, with reasons
--        └──> WEATHER_STAGE ──MERGE──> WEATHER_FACT  (clean, de-duplicated)
--                                          │
--                                      CITY_DIM (static reference data)
--
-- All *_UTC timestamps are UTC. *_LOCAL timestamps are the city's local time (IST).
-- =====================================================================
USE ROLE WEATHER_ETL_ROLE;
USE WAREHOUSE WEATHER_WH;
USE SCHEMA WEATHER_DB.WEATHER;

-- ---------------------------------------------------------------------
-- RAW layer: append-only copy of every Kafka message (valid or not).
-- Kafka partition + offset uniquely identify a message -> full lineage.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_WEATHER (
    KAFKA_TOPIC        VARCHAR(100)   NOT NULL,
    KAFKA_PARTITION    INTEGER        NOT NULL,
    KAFKA_OFFSET       NUMBER(38,0)   NOT NULL,
    KAFKA_KEY          VARCHAR(100),
    KAFKA_TIMESTAMP    TIMESTAMP_NTZ,
    RAW_PAYLOAD        VARCHAR        NOT NULL,     -- original JSON text; parse with TRY_PARSE_JSON
    SPARK_BATCH_ID     NUMBER(38,0),
    LOADED_AT          TIMESTAMP_NTZ  DEFAULT SYSDATE()
)
COMMENT = 'Raw weather events exactly as read from Kafka (includes duplicates and bad records)';

-- ---------------------------------------------------------------------
-- Rejected records: routed here instead of silently disappearing.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS WEATHER_REJECTED (
    KAFKA_PARTITION    INTEGER,
    KAFKA_OFFSET       NUMBER(38,0),
    KAFKA_KEY          VARCHAR(100),
    CITY               VARCHAR(100),
    DQ_REASON          VARCHAR(1000)  NOT NULL,     -- e.g. 'INVALID_HUMIDITY,MISSING_WEATHER_CONDITION'
    RAW_PAYLOAD        VARCHAR,
    SPARK_BATCH_ID     NUMBER(38,0),
    REJECTED_AT        TIMESTAMP_NTZ  DEFAULT SYSDATE()
)
COMMENT = 'Weather events that failed data quality checks';

-- ---------------------------------------------------------------------
-- FACT: grain = ONE weather observation for ONE city at ONE API observation
-- timestamp (OBSERVATION_TS_UTC). EVENT_ID = SHA-256(city | observation time).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS WEATHER_FACT (
    EVENT_ID               VARCHAR(64)    NOT NULL,
    CITY                   VARCHAR(100)   NOT NULL,   -- joins to CITY_DIM.CITY
    COUNTRY                VARCHAR(2),
    API_CITY_ID            NUMBER(38,0),
    LATITUDE               NUMBER(8,4),
    LONGITUDE              NUMBER(8,4),
    OBSERVATION_TS_UTC     TIMESTAMP_NTZ  NOT NULL,
    OBSERVATION_TS_LOCAL   TIMESTAMP_NTZ  NOT NULL,
    OBSERVATION_DATE       DATE           NOT NULL,   -- local date
    OBSERVATION_HOUR       INTEGER        NOT NULL,   -- local hour 0-23
    TEMPERATURE            NUMBER(5,2)    NOT NULL,   -- °C
    FEELS_LIKE             NUMBER(5,2),
    TEMP_MIN               NUMBER(5,2),
    TEMP_MAX               NUMBER(5,2),
    HUMIDITY               INTEGER        NOT NULL,   -- %
    PRESSURE               INTEGER,                   -- hPa
    WIND_SPEED             NUMBER(6,2),               -- m/s
    WIND_DIRECTION         INTEGER,                   -- degrees
    WIND_GUST              NUMBER(6,2),
    CLOUDINESS             INTEGER,                   -- %
    VISIBILITY             INTEGER,                   -- metres (API max 10000)
    RAIN_1H                NUMBER(6,2),               -- mm in last hour (0 if none)
    SNOW_1H                NUMBER(6,2),
    WEATHER_CONDITION      VARCHAR(50)    NOT NULL,
    WEATHER_DESCRIPTION    VARCHAR(200),
    TEMPERATURE_CATEGORY   VARCHAR(20),
    WIND_CATEGORY          VARCHAR(20),
    IS_RAINY               BOOLEAN,
    HEAT_INDEX             NUMBER(5,2),
    HEAT_INDEX_CATEGORY    VARCHAR(20),
    SUNRISE_UTC            TIMESTAMP_NTZ,
    SUNSET_UTC             TIMESTAMP_NTZ,
    -- metadata / lineage
    INGESTION_TS_UTC       TIMESTAMP_NTZ,             -- when Python called the API
    PROCESSING_TS_UTC      TIMESTAMP_NTZ,             -- when Spark processed it
    KAFKA_PARTITION        INTEGER,
    KAFKA_OFFSET           NUMBER(38,0),
    SPARK_BATCH_ID         NUMBER(38,0),
    LOADED_AT              TIMESTAMP_NTZ  DEFAULT SYSDATE(),
    CONSTRAINT PK_WEATHER_FACT PRIMARY KEY (EVENT_ID)   -- informational in Snowflake; MERGE enforces it
)
COMMENT = 'One row per city per API observation timestamp (deduplicated)';

-- No CLUSTER BY: at a few thousand rows per week clustering adds cost, not speed.

-- Transient staging table: Spark truncates + reloads it every micro-batch, then
-- MERGEs it into the fact. Same columns as the fact (LOADED_AT just takes its default).
-- TRANSIENT = no Fail-safe storage cost, which is fine for throw-away staging data.
CREATE TRANSIENT TABLE IF NOT EXISTS WEATHER_STAGE LIKE WEATHER_FACT;

-- ---------------------------------------------------------------------
-- CITY_DIM: small static reference table. Adds attributes the API doesn't
-- give us (state, region, coastal) and a clean slicer table for Power BI.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CITY_DIM (
    CITY          VARCHAR(100)  NOT NULL,
    STATE         VARCHAR(100),
    REGION        VARCHAR(20),
    COUNTRY       VARCHAR(2),
    LATITUDE      NUMBER(8,4),
    LONGITUDE     NUMBER(8,4),
    IS_COASTAL    BOOLEAN,
    CONSTRAINT PK_CITY_DIM PRIMARY KEY (CITY)
)
COMMENT = 'City reference data';

MERGE INTO CITY_DIM t
USING (
    SELECT * FROM VALUES
        ('Delhi',     'Delhi',        'North', 'IN', 28.6667, 77.2167, FALSE),
        ('Mumbai',    'Maharashtra',  'West',  'IN', 19.0144, 72.8479, TRUE),
        ('Hyderabad', 'Telangana',    'South', 'IN', 17.3753, 78.4744, FALSE),
        ('Bengaluru', 'Karnataka',    'South', 'IN', 12.9762, 77.6033, FALSE),
        ('Chennai',   'Tamil Nadu',   'South', 'IN', 13.0878, 80.2785, TRUE),
        ('Kolkata',   'West Bengal',  'East',  'IN', 22.5697, 88.3697, FALSE),
        ('Pune',      'Maharashtra',  'West',  'IN', 18.5196, 73.8553, FALSE)
        AS v(CITY, STATE, REGION, COUNTRY, LATITUDE, LONGITUDE, IS_COASTAL)
) s
ON t.CITY = s.CITY
WHEN MATCHED THEN UPDATE SET STATE = s.STATE, REGION = s.REGION, COUNTRY = s.COUNTRY,
                             LATITUDE = s.LATITUDE, LONGITUDE = s.LONGITUDE, IS_COASTAL = s.IS_COASTAL
WHEN NOT MATCHED THEN INSERT (CITY, STATE, REGION, COUNTRY, LATITUDE, LONGITUDE, IS_COASTAL)
                      VALUES (s.CITY, s.STATE, s.REGION, s.COUNTRY, s.LATITUDE, s.LONGITUDE, s.IS_COASTAL);
```

### `snowflake/03_views.sql`

```sql
-- =====================================================================
-- 03_views.sql  -  Reporting views. Power BI reads ONLY views, never tables:
-- the table can change (new column, rename) without breaking the report.
-- =====================================================================
USE ROLE WEATHER_ETL_ROLE;
USE WAREHOUSE WEATHER_WH;
USE SCHEMA WEATHER_DB.WEATHER;

-- Main fact for Power BI (one row per city per observation).
CREATE OR REPLACE VIEW VW_WEATHER_OBSERVATIONS AS
SELECT
    EVENT_ID,
    CITY,
    OBSERVATION_TS_LOCAL,
    OBSERVATION_DATE,
    OBSERVATION_HOUR,
    TEMPERATURE,
    FEELS_LIKE,
    TEMP_MIN,
    TEMP_MAX,
    HUMIDITY,
    PRESSURE,
    WIND_SPEED,
    WIND_DIRECTION,
    CLOUDINESS,
    VISIBILITY,
    RAIN_1H,
    WEATHER_CONDITION,
    WEATHER_DESCRIPTION,
    TEMPERATURE_CATEGORY,
    WIND_CATEGORY,
    IS_RAINY,
    HEAT_INDEX,
    HEAT_INDEX_CATEGORY,
    OBSERVATION_TS_UTC,
    LOADED_AT
FROM WEATHER_FACT;

-- City dimension for Power BI slicers / map.
CREATE OR REPLACE VIEW VW_CITY AS
SELECT CITY, STATE, REGION, COUNTRY, LATITUDE, LONGITUDE, IS_COASTAL
FROM CITY_DIM;

-- Date dimension for Power BI time intelligence (2025-01-01 .. 2028-12-31).
-- Built in Snowflake (not as a DAX table) so it works in Import AND DirectQuery mode.
CREATE OR REPLACE VIEW VW_DATE AS
WITH d AS (
    SELECT DATEADD('day', ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1, '2025-01-01'::DATE) AS DATE
    FROM TABLE(GENERATOR(ROWCOUNT => 1461))
)
SELECT
    DATE,
    YEAR(DATE)                     AS YEAR,
    MONTH(DATE)                    AS MONTH_NUMBER,
    MONTHNAME(DATE)                AS MONTH_NAME,
    TO_CHAR(DATE, 'YYYY-MM')       AS YEAR_MONTH,
    DAY(DATE)                      AS DAY_OF_MONTH,
    DAYOFWEEKISO(DATE)             AS DAY_OF_WEEK_NUMBER,   -- 1 = Monday
    DAYNAME(DATE)                  AS DAY_NAME,
    WEEKISO(DATE)                  AS ISO_WEEK
FROM d;

-- Latest observation per city (Overview page cards & table).
CREATE OR REPLACE VIEW VW_LATEST_WEATHER AS
SELECT
    f.CITY, d.STATE, d.REGION,
    f.OBSERVATION_TS_LOCAL, f.TEMPERATURE, f.FEELS_LIKE, f.HUMIDITY, f.PRESSURE,
    f.WIND_SPEED, f.WIND_CATEGORY, f.WEATHER_CONDITION, f.WEATHER_DESCRIPTION,
    f.TEMPERATURE_CATEGORY, f.HEAT_INDEX_CATEGORY, f.IS_RAINY,
    DATEDIFF('minute', f.OBSERVATION_TS_UTC, SYSDATE()) AS MINUTES_SINCE_OBSERVATION
FROM WEATHER_FACT f
LEFT JOIN CITY_DIM d ON d.CITY = f.CITY
QUALIFY ROW_NUMBER() OVER (PARTITION BY f.CITY ORDER BY f.OBSERVATION_TS_UTC DESC) = 1;

-- Daily aggregates per city (Temperature Trends page).
CREATE OR REPLACE VIEW VW_DAILY_CITY_WEATHER AS
SELECT
    CITY,
    OBSERVATION_DATE,
    COUNT(*)                          AS OBSERVATION_COUNT,
    ROUND(AVG(TEMPERATURE), 2)        AS AVG_TEMPERATURE,
    MIN(TEMP_MIN)                     AS MIN_TEMPERATURE,
    MAX(TEMP_MAX)                     AS MAX_TEMPERATURE,
    ROUND(AVG(HUMIDITY), 1)           AS AVG_HUMIDITY,
    ROUND(AVG(PRESSURE), 1)           AS AVG_PRESSURE,
    ROUND(AVG(WIND_SPEED), 2)         AS AVG_WIND_SPEED,
    SUM(RAIN_1H)                      AS TOTAL_RAIN_MM_REPORTED,
    COUNT_IF(IS_RAINY)                AS RAINY_OBSERVATIONS,
    MODE(WEATHER_CONDITION)           AS MOST_COMMON_CONDITION
FROM WEATHER_FACT
GROUP BY CITY, OBSERVATION_DATE;

-- Raw JSON made queryable - useful for debugging and replay.
CREATE OR REPLACE VIEW VW_RAW_WEATHER_PARSED AS
SELECT
    KAFKA_PARTITION, KAFKA_OFFSET, KAFKA_TIMESTAMP, LOADED_AT,
    TRY_PARSE_JSON(RAW_PAYLOAD)                         AS PAYLOAD,
    PAYLOAD:city::STRING                                AS CITY,
    PAYLOAD:temperature::FLOAT                          AS TEMPERATURE,
    PAYLOAD:api_timestamp::TIMESTAMP_NTZ                AS API_TIMESTAMP,
    PAYLOAD IS NULL                                     AS IS_MALFORMED
FROM RAW_WEATHER;

-- Data quality summary (rejections by reason and day).
CREATE OR REPLACE VIEW VW_DQ_REJECTION_SUMMARY AS
SELECT
    TO_DATE(REJECTED_AT)     AS REJECTED_DATE,
    r.value::STRING          AS DQ_RULE,
    COUNT(*)                 AS REJECTED_RECORDS
FROM WEATHER_REJECTED,
     LATERAL FLATTEN(INPUT => SPLIT(DQ_REASON, ',')) r
GROUP BY 1, 2;

-- Pipeline health: one row, handy as a "last loaded" card in Power BI.
CREATE OR REPLACE VIEW VW_PIPELINE_HEALTH AS
SELECT
    (SELECT COUNT(*)        FROM RAW_WEATHER)       AS RAW_MESSAGES,
    (SELECT COUNT(*)        FROM WEATHER_FACT)      AS FACT_ROWS,
    (SELECT COUNT(*)        FROM WEATHER_REJECTED)  AS REJECTED_ROWS,
    (SELECT COUNT(DISTINCT CITY) FROM WEATHER_FACT) AS CITIES_LOADED,
    (SELECT MAX(LOADED_AT)  FROM WEATHER_FACT)      AS LAST_FACT_LOAD_UTC,
    (SELECT MAX(OBSERVATION_TS_UTC) FROM WEATHER_FACT) AS LATEST_OBSERVATION_UTC,
    DATEDIFF('minute', (SELECT MAX(LOADED_AT) FROM WEATHER_FACT), SYSDATE()) AS MINUTES_SINCE_LAST_LOAD;

-- Let the BI role read the views (future grants cover new objects, this covers existing ones).
GRANT SELECT ON ALL VIEWS IN SCHEMA WEATHER_DB.WEATHER TO ROLE WEATHER_BI_ROLE;
```

### `spark/snowflake_writer.py`

```python
"""Writes one Spark micro-batch to Snowflake using the Spark-Snowflake connector.

Per micro-batch:
  1. all Kafka messages       -> RAW_WEATHER      (append)
  2. rejected records         -> WEATHER_REJECTED (append)
  3. valid, de-duplicated     -> WEATHER_STAGE    (truncate + load)
  4. MERGE WEATHER_STAGE      -> WEATHER_FACT     (insert only new EVENT_IDs)

Step 4 makes the load idempotent: if Spark re-runs a batch after a crash, or the
API returns the same observation twice, no duplicate fact rows are created.
"""
import logging
import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

log = logging.getLogger("weather_stream.snowflake")

SNOWFLAKE_SOURCE = "net.snowflake.spark.snowflake"

RAW_COLUMNS = {
    "KAFKA_TOPIC": "kafka_topic", "KAFKA_PARTITION": "kafka_partition", "KAFKA_OFFSET": "kafka_offset",
    "KAFKA_KEY": "kafka_key", "KAFKA_TIMESTAMP": "kafka_timestamp", "RAW_PAYLOAD": "raw_payload",
}

REJECTED_COLUMNS = {
    "KAFKA_PARTITION": "kafka_partition", "KAFKA_OFFSET": "kafka_offset", "KAFKA_KEY": "kafka_key",
    "CITY": "city", "DQ_REASON": "dq_reason", "RAW_PAYLOAD": "raw_payload",
}

# Snowflake column -> Spark column. Order doesn't matter (column_mapping=name).
FACT_COLUMNS = {
    "EVENT_ID": "event_id", "CITY": "city", "COUNTRY": "country", "API_CITY_ID": "city_id",
    "LATITUDE": "latitude", "LONGITUDE": "longitude",
    "OBSERVATION_TS_UTC": "api_timestamp", "OBSERVATION_TS_LOCAL": "observation_ts_local",
    "OBSERVATION_DATE": "observation_date", "OBSERVATION_HOUR": "observation_hour",
    "TEMPERATURE": "temperature", "FEELS_LIKE": "feels_like", "TEMP_MIN": "temp_min", "TEMP_MAX": "temp_max",
    "HUMIDITY": "humidity", "PRESSURE": "pressure", "WIND_SPEED": "wind_speed",
    "WIND_DIRECTION": "wind_direction", "WIND_GUST": "wind_gust", "CLOUDINESS": "cloudiness",
    "VISIBILITY": "visibility", "RAIN_1H": "rain_1h", "SNOW_1H": "snow_1h",
    "WEATHER_CONDITION": "weather_condition", "WEATHER_DESCRIPTION": "weather_description",
    "TEMPERATURE_CATEGORY": "temperature_category", "WIND_CATEGORY": "wind_category",
    "IS_RAINY": "is_rainy", "HEAT_INDEX": "heat_index", "HEAT_INDEX_CATEGORY": "heat_index_category",
    "SUNRISE_UTC": "sunrise", "SUNSET_UTC": "sunset",
    "INGESTION_TS_UTC": "ingestion_timestamp", "PROCESSING_TS_UTC": "processing_timestamp",
    "KAFKA_PARTITION": "kafka_partition", "KAFKA_OFFSET": "kafka_offset",
}


def snowflake_options():
    """Connection options from environment variables (.env). Nothing hard-coded."""
    missing = [v for v in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_DATABASE",
                           "SNOWFLAKE_SCHEMA", "SNOWFLAKE_WAREHOUSE") if not os.getenv(v)]
    if missing:
        raise RuntimeError(f"Missing Snowflake settings in .env: {', '.join(missing)}")
    options = {
        "sfURL": f"{os.environ['SNOWFLAKE_ACCOUNT']}.snowflakecomputing.com",
        "sfUser": os.environ["SNOWFLAKE_USER"],
        "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
        "sfDatabase": os.environ["SNOWFLAKE_DATABASE"],
        "sfSchema": os.environ["SNOWFLAKE_SCHEMA"],
        "sfWarehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "sfTimezone": "UTC",
    }
    if os.getenv("SNOWFLAKE_ROLE"):
        options["sfRole"] = os.environ["SNOWFLAKE_ROLE"]
    return options


def _select_as(df: DataFrame, mapping: dict, batch_id: int) -> DataFrame:
    cols = [F.col(src).alias(dst) for dst, src in mapping.items()]
    return df.select(*cols, F.lit(batch_id).cast("long").alias("SPARK_BATCH_ID"))


def _write(df: DataFrame, table: str, options: dict, mode: str = "append", **extra):
    (df.write.format(SNOWFLAKE_SOURCE)
       .options(**options)
       .option("dbtable", table)
       .option("column_mapping", "name")   # match DataFrame columns to table columns by name;
       .options(**extra)                   # table columns not in the DataFrame get their DEFAULT
       .mode(mode)
       .save())


def _merge_sql():
    cols = list(FACT_COLUMNS) + ["SPARK_BATCH_ID"]
    return f"""
        MERGE INTO WEATHER_FACT t
        USING (
            SELECT * FROM WEATHER_STAGE
            QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_ID ORDER BY INGESTION_TS_UTC DESC) = 1
        ) s
        ON t.EVENT_ID = s.EVENT_ID
        WHEN NOT MATCHED THEN INSERT ({", ".join(cols)})
        VALUES ({", ".join("s." + c for c in cols)})
    """


def _run_query(spark, options, sql):
    """Runs a SQL statement in Snowflake through the connector's JVM utility.
    Returns the first column of the first result row (MERGE -> rows inserted), or None."""
    result = spark._jvm.net.snowflake.spark.snowflake.Utils.runQuery(options, sql)
    try:
        if result is not None and result.next():
            return result.getLong(1)
    except Exception:  # result shape differs between connector versions; the count is only for logging
        return None
    return None


def write_batch_to_snowflake(spark, all_df, valid_df, rejected_df, batch_id, options):
    """Returns a dict of counts for logging."""
    counts = {"raw_written": all_df.count(), "rejected_written": rejected_df.count(),
              "staged": valid_df.count(), "fact_inserted": 0}

    _write(_select_as(all_df, RAW_COLUMNS, batch_id), "RAW_WEATHER", options)
    if counts["rejected_written"]:
        _write(_select_as(rejected_df, REJECTED_COLUMNS, batch_id), "WEATHER_REJECTED", options)
    if counts["staged"]:
        # overwrite + truncate_table=on -> TRUNCATE then load; keeps the table definition.
        _write(_select_as(valid_df, FACT_COLUMNS, batch_id), "WEATHER_STAGE", options,
               mode="overwrite", truncate_table="on", usestagingtable="off")
        counts["fact_inserted"] = _run_query(spark, options, _merge_sql())
    return counts
```

### Run it

Fill in the `SNOWFLAKE_*` values in `.env`, set `SINK_MODE=snowflake`, then:
```powershell
cd docker
docker compose --profile spark up --force-recreate spark
```
Expected: `BATCH n SUMMARY ... raw_written=7 ... fact_inserted=7`

---

## Phase 8: Data validation

Guide: [docs/phase-08-data-validation.md](phase-08-data-validation.md)

The rules (`spark/data_quality.py`) were created in Phase 5. Add the bad-data test script and the validation queries:

### `scripts/produce_bad_events.py`

```python
"""Phase 8 test: publish deliberately broken events so you can watch them get rejected.

    python scripts/produce_bad_events.py

Each test case says which DQ rule should catch it. After Spark's next micro-batch,
they should appear in the console output (SINK_MODE=console) or in WEATHER_REJECTED.
"""
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
GOOD = {
    "schema_version": 1, "source": "test", "city": "Delhi", "country": "IN", "latitude": 28.61, "longitude": 77.2,
    "api_timestamp": now, "timezone_offset_seconds": 19800, "temperature": 31.5, "feels_like": 33.1,
    "temp_min": 30.0, "temp_max": 33.0, "humidity": 65, "pressure": 1002, "wind_speed": 4.2,
    "wind_direction": 180, "cloudiness": 40, "visibility": 8000, "weather_condition": "Clouds",
    "weather_description": "scattered clouds", "ingestion_timestamp": now,
}

CASES = [
    ("MALFORMED_JSON", "this is not json {"),
    ("NULL_CITY", {**GOOD, "city": None}),
    ("NULL_OR_BAD_API_TIMESTAMP", {**GOOD, "city": "Mumbai", "api_timestamp": "yesterday-ish"}),
    ("INVALID_TEMPERATURE", {**GOOD, "city": "Pune", "temperature": 999}),
    ("INVALID_HUMIDITY", {**GOOD, "city": "Chennai", "humidity": 150}),
    ("MISSING_WEATHER_CONDITION", {**GOOD, "city": "Kolkata", "weather_condition": "  "}),
    ("INVALID_LATITUDE + INVALID_LONGITUDE", {**GOOD, "city": "Hyderabad", "latitude": 123, "longitude": 500}),
    ("valid (sent twice -> in-batch duplicate, loaded once)", {**GOOD, "city": "bengaluru "}),
    ("valid (sent twice -> in-batch duplicate, loaded once)", {**GOOD, "city": "Bengaluru"}),
]


def main():
    producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")})
    topic = os.getenv("KAFKA_TOPIC", "weather-data")
    for expected, payload in CASES:
        value = payload if isinstance(payload, str) else json.dumps(payload)
        key = payload.get("city") if isinstance(payload, dict) else None
        producer.produce(topic, key=(key or "unknown").strip().encode(), value=value.encode())
        print(f"sent  -> expect {expected}")
    producer.flush(10)
    print(f"\nSent {len(CASES)} test events to '{topic}'. Watch the Spark logs for REJECTED lines.")


if __name__ == "__main__":
    main()
```

### `snowflake/04_validation_queries.sql`

```sql
-- =====================================================================
-- 04_validation_queries.sql  -  Run these after the pipeline has loaded data.
-- Each query says what a "good" result looks like.
-- =====================================================================
USE ROLE WEATHER_ETL_ROLE;
USE WAREHOUSE WEATHER_WH;
USE SCHEMA WEATHER_DB.WEATHER;

-- 1. Row counts per layer. Expect RAW >= FACT + REJECTED
--    (RAW also holds repeats of the same observation, which the MERGE skips).
SELECT * FROM VW_PIPELINE_HEALTH;

-- 2. No duplicate business keys in the fact. Expect: 0 rows.
SELECT EVENT_ID, COUNT(*) FROM WEATHER_FACT GROUP BY EVENT_ID HAVING COUNT(*) > 1;

-- 3. Also no duplicate (city, observation time). Expect: 0 rows.
SELECT CITY, OBSERVATION_TS_UTC, COUNT(*)
FROM WEATHER_FACT GROUP BY 1, 2 HAVING COUNT(*) > 1;

-- 4. Every configured city is arriving, and recently. Expect 7 rows,
--    MINUTES_SINCE_OBSERVATION mostly < 30 while the pipeline runs.
SELECT CITY, OBSERVATION_TS_LOCAL, TEMPERATURE, MINUTES_SINCE_OBSERVATION
FROM VW_LATEST_WEATHER ORDER BY CITY;

-- 5. Nothing that should have been rejected made it into the fact. Expect: 0.
SELECT COUNT(*) AS BAD_ROWS_IN_FACT
FROM WEATHER_FACT
WHERE CITY IS NULL OR OBSERVATION_TS_UTC IS NULL
   OR TEMPERATURE NOT BETWEEN -60 AND 60
   OR HUMIDITY NOT BETWEEN 0 AND 100
   OR LATITUDE NOT BETWEEN -90 AND 90 OR LONGITUDE NOT BETWEEN -180 AND 180
   OR WEATHER_CONDITION IS NULL;

-- 6. Every fact city exists in CITY_DIM (Power BI relationship will work). Expect: 0 rows.
SELECT DISTINCT f.CITY FROM WEATHER_FACT f
LEFT JOIN CITY_DIM d ON d.CITY = f.CITY WHERE d.CITY IS NULL;

-- 7. Why were records rejected?
SELECT * FROM VW_DQ_REJECTION_SUMMARY ORDER BY REJECTED_DATE DESC, REJECTED_RECORDS DESC;
SELECT DQ_REASON, RAW_PAYLOAD, REJECTED_AT FROM WEATHER_REJECTED ORDER BY REJECTED_AT DESC LIMIT 20;

-- 8. Reconciliation: every Kafka message is accounted for exactly once
--    (either a fact row, a rejected row, or a duplicate of an existing fact row).
--    Expect UNACCOUNTED = 0.
WITH raw AS (
    SELECT DISTINCT KAFKA_PARTITION, KAFKA_OFFSET FROM RAW_WEATHER
),
accounted AS (
    SELECT KAFKA_PARTITION, KAFKA_OFFSET FROM WEATHER_REJECTED
    UNION
    SELECT r.KAFKA_PARTITION, r.KAFKA_OFFSET
    FROM VW_RAW_WEATHER_PARSED r
    JOIN WEATHER_FACT f
      ON f.CITY = INITCAP(TRIM(r.CITY)) AND f.OBSERVATION_TS_UTC = r.API_TIMESTAMP
)
SELECT
    (SELECT COUNT(*) FROM raw)                         AS RAW_MESSAGES,
    (SELECT COUNT(*) FROM accounted)                   AS ACCOUNTED,
    (SELECT COUNT(*) FROM raw) - (SELECT COUNT(*) FROM accounted) AS UNACCOUNTED;

-- 9. End-to-end latency: API call -> row in Snowflake (minutes).
SELECT CITY,
       ROUND(AVG(DATEDIFF('second', INGESTION_TS_UTC, LOADED_AT)) / 60, 1) AS AVG_LATENCY_MIN,
       MAX(DATEDIFF('second', INGESTION_TS_UTC, LOADED_AT)) / 60           AS MAX_LATENCY_MIN
FROM WEATHER_FACT
WHERE LOADED_AT > DATEADD('hour', -24, SYSDATE())
GROUP BY CITY ORDER BY CITY;

-- 10. Warehouse cost check: credits used by the POC warehouse in the last 7 days.
--     (ACCOUNT_USAGE has up to ~3h delay; needs ACCOUNTADMIN.)
-- USE ROLE ACCOUNTADMIN;
-- SELECT TO_DATE(START_TIME) AS DAY, SUM(CREDITS_USED) AS CREDITS
-- FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
-- WHERE WAREHOUSE_NAME = 'WEATHER_WH' AND START_TIME > DATEADD('day', -7, CURRENT_TIMESTAMP())
-- GROUP BY 1 ORDER BY 1;
```

### Run it

```powershell
python scripts/produce_bad_events.py      # Spark logs 7 REJECTED lines
```
Then run `04_validation_queries.sql` in Snowsight.

---

## Phases 9 and 10: Power BI connection and dashboard

Guide: [docs/phase-09-powerbi-connection.md](phase-09-powerbi-connection.md)

There's no code to run here. These are reference files for building the report in Power BI Desktop (steps in the Phase 9 and 10 guides):

### `powerbi/dax_measures.md`

````markdown
# DAX measures

Model tables (see Phase 9):

| Power BI table | Snowflake source | Role |
|---|---|---|
| `Weather` | `VW_WEATHER_OBSERVATIONS` | Fact: one row per city per observation |
| `City` | `VW_CITY` | Dimension: slicer, map |
| `Date` | `VW_DATE` | Dimension: marked as date table |
| `Latest` | `VW_LATEST_WEATHER` | Latest row per city (overview page) |
| `Pipeline Health` | `VW_PIPELINE_HEALTH` | One row of load statistics |

Relationships: `City[CITY]` 1→* `Weather[CITY]`, `Date[DATE]` 1→* `Weather[OBSERVATION_DATE]`, `City[CITY]` 1→1 `Latest[CITY]`.

Create a table `_Measures` (Home → Enter data → Load) and add each measure below with **New measure**. The display folders are only a suggestion.

---

## Core

```DAX
Observation Count = COUNTROWS ( Weather )
```
```DAX
Average Temperature = AVERAGE ( Weather[TEMPERATURE] )
```
```DAX
Maximum Temperature = MAX ( Weather[TEMP_MAX] )
```
```DAX
Minimum Temperature = MIN ( Weather[TEMP_MIN] )
```
```DAX
Average Feels Like = AVERAGE ( Weather[FEELS_LIKE] )
```
```DAX
Average Humidity = AVERAGE ( Weather[HUMIDITY] )
```
```DAX
Average Wind Speed = AVERAGE ( Weather[WIND_SPEED] )
```
```DAX
Average Pressure = AVERAGE ( Weather[PRESSURE] )
```
```DAX
Rainy Observations =
CALCULATE ( [Observation Count], Weather[IS_RAINY] = TRUE () )
```
```DAX
Rainy Observation % = DIVIDE ( [Rainy Observations], [Observation Count] )
```
Format: Percentage, 1 decimal.
```DAX
Total Rain (mm) = SUM ( Weather[RAIN_1H] )
```
> `RAIN_1H` is "rain in the last hour" at each observation. Summing several observations within the same hour over-counts, so treat this as an *indicator* (did it rain a lot?), not exact rainfall.

## Latest values (correct in any filter context)

Pattern: for each city in the current filter, find that city's latest observation time and take the value at that time, then average across cities (for one city, that's just its value).

```DAX
Latest Observation Time = MAX ( Weather[OBSERVATION_TS_LOCAL] )
```
```DAX
Latest Temperature =
AVERAGEX (
    VALUES ( City[CITY] ),
    VAR LastTs = CALCULATE ( MAX ( Weather[OBSERVATION_TS_LOCAL] ) )
    RETURN CALCULATE ( MAX ( Weather[TEMPERATURE] ), Weather[OBSERVATION_TS_LOCAL] = LastTs )
)
```
```DAX
Latest Humidity =
AVERAGEX (
    VALUES ( City[CITY] ),
    VAR LastTs = CALCULATE ( MAX ( Weather[OBSERVATION_TS_LOCAL] ) )
    RETURN CALCULATE ( MAX ( Weather[HUMIDITY] ), Weather[OBSERVATION_TS_LOCAL] = LastTs )
)
```
```DAX
Latest Wind Speed =
AVERAGEX (
    VALUES ( City[CITY] ),
    VAR LastTs = CALCULATE ( MAX ( Weather[OBSERVATION_TS_LOCAL] ) )
    RETURN CALCULATE ( MAX ( Weather[WIND_SPEED] ), Weather[OBSERVATION_TS_LOCAL] = LastTs )
)
```
```DAX
Latest Pressure =
AVERAGEX (
    VALUES ( City[CITY] ),
    VAR LastTs = CALCULATE ( MAX ( Weather[OBSERVATION_TS_LOCAL] ) )
    RETURN CALCULATE ( MAX ( Weather[PRESSURE] ), Weather[OBSERVATION_TS_LOCAL] = LastTs )
)
```
```DAX
Latest Condition =
IF (
    HASONEVALUE ( City[CITY] ),
    VAR LastTs = MAX ( Weather[OBSERVATION_TS_LOCAL] )
    RETURN CALCULATE ( MAX ( Weather[WEATHER_DESCRIPTION] ), Weather[OBSERVATION_TS_LOCAL] = LastTs ),
    "Select a city"
)
```

## Trends

```DAX
Avg Temp Previous Day =
CALCULATE ( [Average Temperature], DATEADD ( 'Date'[DATE], -1, DAY ) )
```
```DAX
Temp Change vs Previous Day =
VAR CurrentAvg = [Average Temperature]
VAR PreviousAvg = [Avg Temp Previous Day]
RETURN IF ( NOT ISBLANK ( CurrentAvg ) && NOT ISBLANK ( PreviousAvg ), CurrentAvg - PreviousAvg )
```
Format: `+0.0 °C;-0.0 °C;0.0 °C`.
```DAX
Daily Temperature Range =
AVERAGEX (
    VALUES ( 'Date'[DATE] ),
    CALCULATE ( MAX ( Weather[TEMP_MAX] ) - MIN ( Weather[TEMP_MIN] ) )
)
```
```DAX
Temp 7-Day Moving Avg =
VAR LastDate = MAX ( 'Date'[DATE] )
RETURN
CALCULATE (
    [Average Temperature],
    DATESINPERIOD ( 'Date'[DATE], LastDate, -7, DAY )
)
```

## Freshness / pipeline

```DAX
Minutes Since Last Observation =
VAR LastUtc = CALCULATE ( MAX ( Weather[OBSERVATION_TS_UTC] ), REMOVEFILTERS () )
RETURN DATEDIFF ( LastUtc, UTCNOW (), MINUTE )
```
> In Import mode this is measured against the time the visual is *rendered*, so it grows between refreshes. That's useful: a large number means "refresh me" or "pipeline stopped".
```DAX
Data Status =
SWITCH (
    TRUE (),
    [Minutes Since Last Observation] <= 30, "🟢 Live",
    [Minutes Since Last Observation] <= 120, "🟡 Delayed",
    "🔴 Stale"
)
```
```DAX
Last Refreshed (UTC) = "Data as of " & FORMAT ( CALCULATE ( MAX ( Weather[LOADED_AT] ), REMOVEFILTERS () ), "dd-mmm-yyyy hh:nn" ) & " UTC"
```

## Presentation helpers

```DAX
Selected City Title =
"Weather details – " & SELECTEDVALUE ( City[CITY], "select a city" )
```
```DAX
Latest Temp Color =
VAR t = [Latest Temperature]
RETURN
SWITCH (
    TRUE (),
    ISBLANK ( t ), "#9E9E9E",
    t >= 38, "#C62828",
    t >= 30, "#EF6C00",
    t >= 20, "#F9A825",
    t >= 10, "#2E7D32",
    "#1565C0"
)
```
Use it via *Format → Conditional formatting → Field value* on cards, bars or table cells.

---

### Why these patterns
* **`AVERAGEX(VALUES(City[CITY]), ...)` for "latest"**: a plain `MAX(timestamp)` at the total level only picks cities that reported at the exact global max time. Iterating cities gives each city its own latest reading.
* **`CALCULATE` inside the iterator** triggers *context transition*: the current city becomes a filter.
* **`DATEADD` / `DATESINPERIOD`** need the `Date` table marked as a date table with a contiguous range. `VW_DATE` provides 2025–2028.
* All measures work in **Import and DirectQuery** (no calculated tables or columns required).
````

### `powerbi/dashboard_design.md`

````markdown
# Dashboard design (4 pages)

Canvas: 16:9 (1280 × 720). Top band on every page (height ~60 px): title on the left, the `[Data Status]` card plus the `[Last Refreshed (UTC)]` card on the right. Use a consistent colour per city (Format → Data colors) so Delhi is the same colour everywhere.

**Slicers used across pages:** `City[CITY]` (dropdown, multi-select), `Date[DATE]` (between slider), `Weather[WEATHER_CONDITION]` (dropdown). Use **View → Sync slicers** so City and Date stay the same on pages 1–3. Page 4 has its own single-select City slicer.

---

## Page 1: Weather Overview
*Question it answers: what's the weather right now, everywhere?*

```
┌─────────────────────────────────────────────────────────────────────┐
│ Weather Overview                          [🟢 Live] [Data as of …]  │
├──────────┬──────────┬──────────┬──────────┬─────────────────────────┤
│ Latest   │ Latest   │ Latest   │ Latest   │  Slicers: City | Date   │
│ Temp °C  │ Humidity │ Wind m/s │ Obs time │                         │
├──────────┴──────────┴──────────┴──────────┼─────────────────────────┤
│ Clustered bar: Latest Temperature by City │  Map (City lat/long,    │
│ (sorted desc, colour = Latest Temp Color) │  bubble size = Latest   │
│                                           │  Temperature, tooltip = │
├───────────────────────────────────────────┤  condition)             │
│ Table: City | Latest Temp | Feels like |  │                         │
│ Humidity | Wind | Condition | Obs time    │                         │
│ (source: Latest table + City)             │                         │
└───────────────────────────────────────────┴─────────────────────────┘
```

| Visual | Fields |
|---|---|
| 4 cards | `[Latest Temperature]`, `[Latest Humidity]`, `[Latest Wind Speed]`, `[Latest Observation Time]` |
| Clustered bar | Y: `City[CITY]`; X: `[Latest Temperature]`; bar colour: conditional formatting by `[Latest Temp Color]` |
| Map (or Azure Map) | Latitude/Longitude: `City[LATITUDE]`/`City[LONGITUDE]`; Size: `[Latest Temperature]`; Tooltips: `Latest[WEATHER_DESCRIPTION]`, `[Latest Humidity]` |
| Table | `City[CITY]`, `Latest[TEMPERATURE]`, `Latest[FEELS_LIKE]`, `Latest[HUMIDITY]`, `Latest[WIND_SPEED]`, `Latest[WEATHER_CONDITION]`, `Latest[OBSERVATION_TS_LOCAL]`, `Latest[HEAT_INDEX_CATEGORY]` |

> Map visuals need *File → Options → Security → Use Map and Filled Map visuals* enabled. If your tenant disables them, use a scatter chart (lon on X, lat on Y) instead.

## Page 2: Temperature Trends
*Question: how is temperature changing over time and between cities?*

| Visual | Fields |
|---|---|
| Line chart (full width) | X: `Weather[OBSERVATION_TS_LOCAL]` (continuous axis); Y: `[Average Temperature]`; Legend: `City[CITY]`. Analytics pane → **Trend line** on |
| Clustered column | X: `Date[DATE]`; Y: `[Average Temperature]`; Legend: `City[CITY]` (daily average) |
| Line + clustered column (or area) | X: `Date[DATE]`; Y: `[Minimum Temperature]`, `[Maximum Temperature]`; one city at a time via the slicer |
| Matrix | Rows: `City[CITY]`; Columns: `Date[DATE]`; Values: `[Average Temperature]` with a background colour scale (heatmap) |
| KPI cards | `[Maximum Temperature]`, `[Minimum Temperature]`, `[Temp Change vs Previous Day]`, `[Temp 7-Day Moving Avg]` |

## Page 3: Weather Conditions
*Question: what kind of weather are we seeing, and how are humidity, pressure, wind and rain behaving?*

| Visual | Fields |
|---|---|
| Donut | Legend: `Weather[WEATHER_CONDITION]`; Values: `[Observation Count]` |
| 100% stacked bar | Y: `City[CITY]`; X: `[Observation Count]`; Legend: `Weather[WEATHER_CONDITION]` |
| Line chart | X: `Weather[OBSERVATION_TS_LOCAL]`; Y: `[Average Humidity]`; Legend: City |
| Line chart | X: `Weather[OBSERVATION_TS_LOCAL]`; Y: `[Average Pressure]`; Legend: City |
| Line chart | X: `Weather[OBSERVATION_TS_LOCAL]`; Y: `[Average Wind Speed]`; Legend: City |
| Clustered column | X: `City[CITY]`; Y: `[Rainy Observations]`; tooltip `[Rainy Observation %]` |
| Slicer | `Weather[WEATHER_CONDITION]` |

## Page 4: City Detail
*Question: tell me everything about one city.*

| Visual | Fields |
|---|---|
| Slicer (single select, tile style) | `City[CITY]`. **Edit interactions**/sync so it only affects this page |
| Title text box | Dynamic title: a card with `[Selected City Title]` |
| Cards | `[Latest Temperature]`, `[Latest Humidity]`, `[Latest Wind Speed]`, `[Latest Pressure]`, `[Latest Condition]`, `City[STATE]`, `City[REGION]` |
| Line chart | Temperature history: X `OBSERVATION_TS_LOCAL`, Y `[Average Temperature]` and `[Average Feels Like]` |
| Line chart | Humidity history |
| Line chart | Wind speed history |
| Line chart | Pressure history |
| Stacked column | X: `Date[DATE]`, Y: `[Observation Count]`, Legend: `Weather[WEATHER_CONDITION]` (conditions per day) |

**Drill-through (nice touch):** on Page 4, add `City[CITY]` to the *Drill through* well. Users can then right-click any city on pages 1–3 → *Drill through → City Detail*.

---

## Finishing checklist
- [ ] All temperatures formatted `0.0 "°C"`, humidity `0"%"`, wind `0.0 "m/s"`
- [ ] Axis titles removed where the visual title says it all
- [ ] Tooltips show local time, not UTC
- [ ] Every page's visuals respond to the slicers as intended (test with Delhi only)
- [ ] Page navigation buttons (Insert → Buttons → Navigator → Page navigator)
- [ ] Screenshots of each page saved in `docs/images/` for the README
````

---

## Phases 11 and 12: End-to-end testing and documentation

Guide: [docs/phase-11-end-to-end-testing.md](phase-11-end-to-end-testing.md)

Phase 11 adds no new code. It runs everything together (see its guide). For Phase 12, create the project README:

### `README.md`

````markdown
# Dynamic Weather Data Pipeline: Kafka → Spark → Snowflake → Power BI

A near-real-time data engineering POC that polls live weather for 7 Indian cities, streams it through **Apache Kafka**, cleans and validates it with **PySpark Structured Streaming**, loads it idempotently into **Snowflake**, and visualises it in a 4-page **Power BI** dashboard. Built to run on one Windows laptop with Docker.

```
OpenWeatherMap ─► Python producer ─► Kafka ─► Spark Structured Streaming ─► Snowflake ─► Power BI
  (REST API)      retries, timeout,   topic    parse · clean · enrich ·     RAW → FACT     4-page
                  validation, logs    weather-  data quality · dedup        (MERGE),        dashboard
                                      data      foreachBatch               views
```

| | |
|---|---|
| **Cities** | Delhi, Mumbai, Hyderabad, Bengaluru, Chennai, Kolkata, Pune (configurable in `config/cities.json`) |
| **Polling** | every 5 min (`POLL_INTERVAL_SECONDS`) |
| **Latency** | API → Snowflake ≈ 1–2 min (1-minute Spark trigger) |
| **Guarantees** | no duplicate facts (deterministic `event_id` + idempotent MERGE); bad records kept with reasons |
| **Stack** | Python 3.11 · confluent-kafka · Kafka 3.9 (KRaft, Docker) · Spark 3.5 (Docker) · Snowflake · Power BI Desktop |

## Repository layout
```
weather-etl-pipeline/
├── .env.example               all settings; copy to .env (git-ignored)
├── requirements.txt           ingestion + tests
├── requirements-dev.txt       + pyspark for local Spark unit tests (optional)
├── config/cities.json         cities to poll
├── docker/docker-compose.yml  Kafka, Kafka UI, Spark
├── ingestion/                 API → Kafka
│   ├── config.py              settings from .env
│   ├── weather_api.py         HTTP client (timeout, retries) + event mapping
│   ├── kafka_producer.py      idempotent producer with delivery tracking
│   ├── mock_weather.py        offline fake API
│   └── main.py                polling loop (--once, --dry-run, --mock)
├── spark/                     Kafka → Snowflake
│   ├── weather_stream.py      streaming job (foreachBatch)
│   ├── schemas.py             explicit event schema
│   ├── transformations.py     parse, clean, derive
│   ├── data_quality.py        9 DQ rules, reject routing
│   └── snowflake_writer.py    RAW / REJECTED / STAGE → MERGE → FACT
├── snowflake/                 01_setup · 02_tables · 03_views · 04_validation_queries
├── powerbi/                   dax_measures.md · dashboard_design.md
├── scripts/                   consume_test.py · produce_bad_events.py
├── tests/                     unit tests (ingestion + Spark transformations)
└── docs/                      architecture + a step-by-step guide per phase
```

## Build it phase by phase

**Want to type or paste the code yourself in VS Code?** [docs/BUILD_GUIDE.md](docs/BUILD_GUIDE.md) has every file's full code, grouped by the phase where you create it.

| # | Phase | Guide |
|---|---|---|
| – | Architecture and design decisions | [docs/00-architecture.md](docs/00-architecture.md) |
| 1 | Project setup (Windows, venv, `.env`) | [phase-01](docs/phase-01-project-setup.md) |
| 2 | Weather API ingestion | [phase-02](docs/phase-02-weather-api-ingestion.md) |
| 3 | Kafka (Docker) and producer | [phase-03](docs/phase-03-kafka-producer.md) |
| 4 | Kafka topic and consumer validation | [phase-04](docs/phase-04-kafka-topic-consumer-validation.md) |
| 5 | Spark Structured Streaming | [phase-05](docs/phase-05-spark-structured-streaming.md) |
| 6 | Cleaning and transformation | [phase-06](docs/phase-06-cleaning-transformation.md) |
| 7 | Snowflake integration | [phase-07](docs/phase-07-snowflake-integration.md) |
| 8 | Data quality and validation | [phase-08](docs/phase-08-data-validation.md) |
| 9 | Power BI connection and model | [phase-09](docs/phase-09-powerbi-connection.md) |
| 10 | Dashboard development | [phase-10](docs/phase-10-dashboard-development.md) |
| 11 | End-to-end testing and monitoring | [phase-11](docs/phase-11-end-to-end-testing.md) |
| 12 | Documentation and portfolio | [phase-12](docs/phase-12-documentation-portfolio.md) |

Each guide has the same sections: objective · architecture position · prerequisites · files · commands · code · explanation · expected output · testing · common errors · checklist.

## Quick start (after Phase 1 setup)
```powershell
# 1. Kafka
cd docker; docker compose up -d
docker exec weather-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic weather-data --partitions 3 --replication-factor 1

# 2. Spark (SINK_MODE=console or snowflake in .env)
docker compose --profile spark up -d spark; docker logs -f weather-spark

# 3. Producer (new terminal, project root, venv active)
python -m ingestion.main            # add --mock to run without an API key
```
UIs: Kafka UI http://localhost:8080 · Spark UI http://localhost:4040

## Tests
```powershell
pytest -q tests/test_weather_api.py                                   # no extra dependencies
pip install -r requirements-dev.txt; pytest -q                        # + Spark tests (needs Java 17)
```

## Results
_Fill in after running it (Phase 12):_ days of data · fact rows · rejected rows · measured latency · Snowflake credits used · screenshots in `docs/images/`.
````

