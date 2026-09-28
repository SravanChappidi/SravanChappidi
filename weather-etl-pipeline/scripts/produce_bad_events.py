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
