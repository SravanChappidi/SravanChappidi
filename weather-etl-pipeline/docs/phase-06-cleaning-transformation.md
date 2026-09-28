# Phase 6: Data cleaning and transformation

## 1. Objective
Turn parsed events into analysis-ready rows: consistent text, correct data types, explicit NULL handling, and useful derived fields (local date/hour, categories, rain flag, heat index, event ID).

## 2. Architecture position
```
Kafka ──► Spark [parse ──► CLEAN ──► DERIVE] ──► DQ ──► sink
                           ▲ you are here
```

## 3. Prerequisites
Phase 5 running in console mode.

## 4. Folder / file structure
```
spark/transformations.py        clean() and add_derived_fields()   ◄ this phase
tests/test_transformations.py   unit tests on a batch DataFrame    ◄ this phase
```

## 5. Commands
```powershell
# Restart Spark to pick up code changes
cd docker
docker compose --profile spark up --force-recreate spark

# OPTIONAL: run the Spark unit tests locally (needs Java 17 on Windows)
pip install -r requirements-dev.txt
pytest -q tests/test_transformations.py
```

## 6. Code

**Cleaning**: `clean()` (excerpt)
```python
.withColumn("city", F.initcap(F.trim("city")))                 # " delhi " -> "Delhi"
.withColumn("country", F.upper(F.trim("country")))             # "in" -> "IN"
.withColumn("city", F.when(F.col("city") != "", F.col("city")))   # "" -> NULL
.withColumn("api_timestamp", F.to_timestamp("api_timestamp"))  # string -> timestamp (UTC)
.withColumn("humidity", F.round("humidity").cast("int"))       # 65.0 -> 65
.withColumn("rain_1h", F.coalesce("rain_1h", F.lit(0.0)))      # absent -> 0 mm
.withColumn("temperature", F.round("temperature", 2))
```

**Derived fields**: `add_derived_fields()` (excerpt)
```python
.withColumn("event_id", F.sha2(F.concat_ws("|", F.upper("city"),
            F.date_format("api_timestamp", "yyyy-MM-dd'T'HH:mm:ss")), 256))
.withColumn("observation_ts_local",
            F.timestamp_seconds(F.unix_timestamp("api_timestamp") + F.col("timezone_offset_seconds")))
.withColumn("observation_date", F.to_date("observation_ts_local"))
.withColumn("observation_hour", F.hour("observation_ts_local"))
.withColumn("temperature_category",
            F.when(temp < 10, "Cold").when(temp < 20, "Cool").when(temp < 30, "Moderate")
             .when(temp < 38, "Hot").otherwise("Extreme Heat"))
.withColumn("wind_category", ...)                    # simplified Beaufort: Calm/Light/Moderate/Strong/Gale
.withColumn("is_rainy", (F.col("rain_1h") > 0) | F.col("weather_condition").isin("Rain", "Drizzle", "Thunderstorm"))
.withColumn("heat_index", ...)                       # NOAA formula when >= 27°C and >= 40% humidity
.withColumn("heat_index_category", ...)              # Normal / Caution / Extreme Caution / Danger / Extreme Danger
.withColumn("processing_timestamp", F.current_timestamp())
```

## 7. Explanation

| Transformation | Why |
|---|---|
| Trim + `initcap` city, upper country | `"delhi "`, `"Delhi"` and `"DELHI"` must be the same city, otherwise Power BI shows three bars. |
| Empty string → NULL | `""` would pass a `IS NULL` check. Converting makes DQ rules catch it. |
| `to_timestamp` | Strings can't be compared or bucketed by hour. Unparseable → NULL → rejected in Phase 8. |
| Round + cast to int | Humidity/pressure are whole numbers. Types match the Snowflake columns exactly. |
| `rain_1h` NULL → 0 | The API **omits** the rain block when it isn't raining, so absence means 0 mm, not "unknown". This is a business decision, documented here. |
| `event_id` | Deterministic key for **deduplication**: same city + same API observation time = same event, whichever poll delivered it. SHA-256 gives a fixed-length key that's safe to use in joins. |
| Local timestamp / date / hour | Users in India think in IST. "Daily average" must use the **local** day, not the UTC day (which starts at 05:30 IST). |
| Temperature / wind categories | Easy-to-read labels for slicers and colour coding in Power BI. |
| Rain flag | One boolean for "rain occurrences" on Page 3, combining the rain amount and the condition text. |
| Heat index | "Feels like" for heat stress, relevant for Indian summers. Standard NOAA regression, only applied where valid (≥ 27°C, ≥ 40% RH). |
| `processing_timestamp` | Lets you measure latency: API → Kafka → Spark → Snowflake. |

**No Python UDFs.** Everything uses built-in `pyspark.sql.functions`, so it runs inside the JVM (no Python serialization overhead) and Spark's optimizer can see through it.

**Timezone.** `spark.sql.session.timeZone=UTC` is set explicitly. Otherwise Spark uses the container's or laptop's timezone and your timestamps shift by 5.5 hours depending on where the job runs.

**Pure functions.** Each step is `DataFrame -> DataFrame` with no I/O, so it's testable with a normal batch DataFrame. That's how `tests/test_transformations.py` works.

## 8. Expected output
```
+---------+--------------------+-----------+--------+----------+-----------------+--------------------+-------------+--------+-------------------+
|city     |observation_ts_local|temperature|humidity|wind_speed|weather_condition|temperature_category|wind_category|is_rainy|heat_index_category|
+---------+--------------------+-----------+--------+----------+-----------------+--------------------+-------------+--------+-------------------+
|Bengaluru|2026-09-28 14:20:00 |33.75      |37      |6.94      |Rain             |Hot                 |Moderate     |true    |Extreme Caution    |
|Delhi    |2026-09-28 14:20:00 |25.58      |66      |6.21      |Rain             |Moderate            |Moderate     |true    |Normal             |
|Mumbai   |2026-09-28 14:20:00 |38.53      |82      |4.1       |Rain             |Extreme Heat        |Light        |true    |Extreme Danger     |
...
```
`observation_ts_local` = UTC + 5:30.

## 9. Testing steps
1. Unit tests (optional, needs Java): `pytest -q tests/test_transformations.py`. 15 tests cover cleaning, IST conversion, categories, the rain flag, the heat index and dedup.
2. In the console output, check that `observation_ts_local` is 5h30m ahead of the API's UTC time.
3. Send `produce_bad_events.py` (Phase 8). The row with city `"bengaluru "` shows up cleaned as `Bengaluru`.

## 10. Common errors and fixes

| Problem | Fix |
|---|---|
| All timestamps NULL | The producer's timestamp format changed. `to_timestamp` accepts ISO-8601 like `2026-09-28T08:50:00+00:00`. |
| Times 5h30 off | `spark.sql.session.timeZone` isn't UTC, or you're reading `observation_ts_local` as if it were UTC. |
| `AnalysisException: cannot resolve 'xyz'` | Typo in a column name, or the field is missing from `schemas.py`. |
| Local tests: `Java gateway process exited` | Java 17 isn't installed or `JAVA_HOME` isn't set. The tests are optional; Docker Spark doesn't need them. |

## 11. Verify before moving on
- [ ] City names are consistently cased, and rain is 0 rather than NULL when dry
- [ ] Local time/date/hour are correct for IST
- [ ] You can justify each derived field and why it's computed in Spark rather than in the producer
