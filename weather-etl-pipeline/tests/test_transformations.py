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
