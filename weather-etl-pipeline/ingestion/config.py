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
