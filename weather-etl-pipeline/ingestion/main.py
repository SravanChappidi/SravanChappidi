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
