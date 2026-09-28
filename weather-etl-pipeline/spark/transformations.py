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
