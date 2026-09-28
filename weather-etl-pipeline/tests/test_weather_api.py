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
