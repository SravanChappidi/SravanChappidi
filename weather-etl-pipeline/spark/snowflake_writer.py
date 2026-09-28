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
