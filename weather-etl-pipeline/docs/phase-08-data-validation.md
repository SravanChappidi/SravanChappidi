# Phase 8: Data quality and validation

## 1. Objective
Catch bad data in Spark with explicit rules, route bad records (and **why** they're bad) to a separate table, and validate the loaded data in Snowflake with reconciliation queries.

## 2. Architecture position
```
Spark [parse → clean → derive → DATA QUALITY] ──valid──► STAGE → MERGE → WEATHER_FACT
                                     └──────rejected──► WEATHER_REJECTED (+ WARNING log line)
Snowflake: 04_validation_queries.sql checks the result
```

## 3. Prerequisites
Phase 7 running (or Phase 5–6 in console mode; DQ works the same, output goes to the console).

## 4. Folder / file structure
```
spark/data_quality.py               rules, dq_errors, split valid/rejected   ◄ this phase
scripts/produce_bad_events.py       sends one broken event per rule           ◄ this phase
snowflake/04_validation_queries.sql post-load checks                          ◄ this phase
```

## 5. Commands
```powershell
python scripts/produce_bad_events.py        # sends 9 test events
docker logs -f weather-spark                # watch for REJECTED lines
```
Then run `snowflake/04_validation_queries.sql` in Snowsight.

## 6. Code
`spark/data_quality.py`
```python
def dq_rules():
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

def apply_quality_checks(df):
    rule_checks = F.array(*[F.when(condition, F.lit(name)) for name, condition in dq_rules()])
    errors = F.when(~F.col("is_valid_json"), F.array(F.lit("MALFORMED_JSON"))) \
              .otherwise(F.filter(rule_checks, lambda x: x.isNotNull()))
    return (df.withColumn("dq_errors", errors)
              .withColumn("dq_reason", F.array_join("dq_errors", ","))
              .withColumn("is_valid", F.size("dq_errors") == 0))

def split_valid_rejected(checked_df):
    valid = checked_df.filter("is_valid").dropDuplicates(["event_id"])
    rejected = checked_df.filter(~F.col("is_valid"))
    return valid, rejected
```

## 7. Explanation

| Check | Rule | Where handled |
|---|---|---|
| Malformed Kafka message | `get_json_object(raw, '$')` is NULL or doesn't start with `{` | `MALFORMED_JSON` (only reason reported, since all other fields are NULL anyway) |
| Null city | NULL or empty after trim | `NULL_CITY` |
| Null timestamp | missing **or unparseable** (`to_timestamp` → NULL) | `NULL_OR_BAD_API_TIMESTAMP` |
| Invalid temperature | NULL, non-numeric, or outside −60…60 °C | `INVALID_TEMPERATURE` |
| Invalid humidity | NULL or outside 0…100 % | `INVALID_HUMIDITY` |
| Missing weather condition | NULL/blank | `MISSING_WEATHER_CONDITION` |
| Invalid lat/long | NULL or outside ±90 / ±180 | `INVALID_LATITUDE`, `INVALID_LONGITUDE` |
| Duplicates | same `event_id` | not an error: dropped within the batch (`dropDuplicates`), skipped across batches (`MERGE`) and counted in logs as `in_batch_duplicates` |

Design choices:
* **All failures are reported, not just the first.** A record with bad humidity *and* bad latitude gets `INVALID_HUMIDITY,INVALID_LATITUDE`, so you fix the producer once, not twice.
* **Rejects are data.** They go to `WEATHER_REJECTED` with the raw payload, Kafka partition/offset, and reason, plus a `WARNING` log line. `VW_DQ_REJECTION_SUMMARY` counts rejections by rule and day. You can build a DQ page in Power BI from it.
* **Hard vs soft rules.** Pressure is only checked if present (it's optional); temperature and humidity are required because the dashboard can't work without them.
* **Why duplicates aren't "rejected":** a repeated observation isn't bad data. Routing it to rejects would make the DQ numbers look alarming for normal behaviour.
* **Why not Great Expectations?** 9 rules as Spark columns run inside the same job, per record, with the reason attached. GE is great for batch validation suites, but it would add a dependency and a second execution path for no extra value here.

## 8. Expected output
Spark (after `produce_bad_events.py`):
```
WARNING weather_stream - REJECTED batch=5 partition=1 offset=3 city=None reasons=MALFORMED_JSON
WARNING weather_stream - REJECTED batch=5 partition=1 offset=4 city=None reasons=NULL_CITY
WARNING weather_stream - REJECTED batch=5 partition=1 offset=5 city=Kolkata reasons=MISSING_WEATHER_CONDITION
WARNING weather_stream - REJECTED batch=5 partition=1 offset=6 city=Hyderabad reasons=INVALID_LATITUDE,INVALID_LONGITUDE
WARNING weather_stream - REJECTED batch=5 partition=0 offset=3 city=Mumbai reasons=NULL_OR_BAD_API_TIMESTAMP
WARNING weather_stream - REJECTED batch=5 partition=0 offset=4 city=Pune reasons=INVALID_TEMPERATURE
WARNING weather_stream - REJECTED batch=5 partition=0 offset=5 city=Chennai reasons=INVALID_HUMIDITY
INFO    weather_stream - BATCH 5 SUMMARY consumed=9 valid=1 rejected=7 in_batch_duplicates=1 ...
```
9 sent = 7 rejected + 2 valid Bengaluru copies (`"bengaluru "` and `"Bengaluru"` clean to the same `event_id`) → 1 valid row + 1 in-batch duplicate.

Snowflake:
```
SELECT * FROM VW_DQ_REJECTION_SUMMARY;
REJECTED_DATE  DQ_RULE                     REJECTED_RECORDS
2026-09-28     INVALID_LATITUDE            1
2026-09-28     MALFORMED_JSON              1
...
```

## 9. Testing steps
1. Run `produce_bad_events.py` and match each `REJECTED` line to the expected rule it printed.
2. In Snowflake, run every query in `04_validation_queries.sql` and compare with the expected result in its comment:
   * duplicate check → **0 rows**
   * bad rows in fact → **0**
   * every fact city in CITY_DIM → **0 rows**
   * reconciliation → **UNACCOUNTED = 0**
3. The test Bengaluru row *is* valid and lands in the fact table. Remove it if you want a clean demo:
   ```sql
   DELETE FROM WEATHER_FACT f USING RAW_WEATHER r
   WHERE f.KAFKA_PARTITION = r.KAFKA_PARTITION AND f.KAFKA_OFFSET = r.KAFKA_OFFSET
     AND r.RAW_PAYLOAD LIKE '%"source": "test"%';
   ```

## 10. Common errors and fixes

| Problem | Fix |
|---|---|
| Valid records rejected with `INVALID_HUMIDITY` | Your producer sends humidity as a string (`"65"`)? The schema reads it as Double; `"65"` in quotes becomes NULL. Fix the producer. |
| Everything rejected with `NULL_OR_BAD_API_TIMESTAMP` | Timestamp format changed. Check a raw payload in Kafka UI. |
| `UNACCOUNTED > 0` in reconciliation | Usually test events with unusual cities. Inspect: `SELECT * FROM VW_RAW_WEATHER_PARSED WHERE ...` for offsets not in fact/rejected. |

## 11. Verify before moving on
- [ ] Each broken event is rejected with the right reason and visible in `WEATHER_REJECTED`
- [ ] All validation queries return their expected "good" result
- [ ] You can explain why duplicates aren't treated as rejects
