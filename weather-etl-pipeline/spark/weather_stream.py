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
