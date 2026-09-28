"""Rule-based data quality checks.

Every record gets a `dq_errors` array. Empty array -> valid. Non-empty -> the
record is routed to the rejected table WITH the reasons, so nothing silently
disappears and you can query why a record was rejected.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def dq_rules():
    """(rule name, condition that makes the record BAD). A function, not a constant,
    because Column expressions can only be built once a SparkSession exists."""
    return [
        ("NULL_CITY", F.col("city").isNull()),
        ("NULL_OR_BAD_API_TIMESTAMP", F.col("api_timestamp").isNull()),
        ("INVALID_TEMPERATURE", F.col("temperature").isNull() | ~F.col("temperature").between(-60, 60)),
        ("INVALID_HUMIDITY", F.col("humidity").isNull() | ~F.col("humidity").between(0, 100)),
        ("MISSING_WEATHER_CONDITION", F.col("weather_condition").isNull()),
        ("INVALID_LATITUDE", F.col("latitude").isNull() | ~F.col("latitude").between(-90, 90)),
        ("INVALID_LONGITUDE", F.col("longitude").isNull() | ~F.col("longitude").between(-180, 180)),
        ("INVALID_PRESSURE", F.col("pressure").isNotNull() & ~F.col("pressure").between(850, 1100)),
        ("NEGATIVE_WIND_SPEED", F.col("wind_speed") < 0),
    ]


def apply_quality_checks(df: DataFrame) -> DataFrame:
    """Adds dq_errors (array<string>), dq_reason (string) and is_valid (boolean)."""
    rule_checks = F.array(*[F.when(condition, F.lit(name)) for name, condition in dq_rules()])
    errors = F.when(~F.col("is_valid_json"), F.array(F.lit("MALFORMED_JSON"))) \
              .otherwise(F.filter(rule_checks, lambda x: x.isNotNull()))
    return (
        df.withColumn("dq_errors", errors)
          .withColumn("dq_reason", F.array_join("dq_errors", ","))
          .withColumn("is_valid", F.size("dq_errors") == 0)
    )


def split_valid_rejected(checked_df: DataFrame):
    """Returns (valid_df, rejected_df). valid_df is de-duplicated on event_id within the batch."""
    valid = checked_df.filter("is_valid").dropDuplicates(["event_id"])
    rejected = checked_df.filter(~F.col("is_valid"))
    return valid, rejected
