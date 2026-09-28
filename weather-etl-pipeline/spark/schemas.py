"""Explicit schema of the JSON event produced by ingestion/weather_api.py.

Why explicit? Streaming sources cannot infer schemas, and an explicit contract
means a producer change (renamed/removed field) shows up as NULLs that our data
quality checks catch - instead of silently changing the table structure.

Numbers are read as DoubleType and timestamps as strings on purpose: a humidity
of 65.0 or a timestamp in an unexpected format should not make the whole
record unreadable. We cast to the final types in transformations.clean().
"""
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

WEATHER_EVENT_SCHEMA = StructType([
    StructField("schema_version", IntegerType()),
    StructField("source", StringType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("city_id", LongType()),
    StructField("api_location_name", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("api_timestamp", StringType()),
    StructField("timezone_offset_seconds", DoubleType()),
    StructField("temperature", DoubleType()),
    StructField("feels_like", DoubleType()),
    StructField("temp_min", DoubleType()),
    StructField("temp_max", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("pressure", DoubleType()),
    StructField("wind_speed", DoubleType()),
    StructField("wind_direction", DoubleType()),
    StructField("wind_gust", DoubleType()),
    StructField("cloudiness", DoubleType()),
    StructField("visibility", DoubleType()),
    StructField("rain_1h", DoubleType()),
    StructField("snow_1h", DoubleType()),
    StructField("weather_condition", StringType()),
    StructField("weather_description", StringType()),
    StructField("sunrise", StringType()),
    StructField("sunset", StringType()),
    StructField("ingestion_timestamp", StringType()),
])
