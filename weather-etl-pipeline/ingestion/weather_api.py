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
