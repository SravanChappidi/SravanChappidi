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
