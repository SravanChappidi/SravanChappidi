# Architecture: what each component does, and why it's there

## The flow

```
 OpenWeatherMap API  (HTTP, JSON, 1 call per city per poll)
        │
        ▼
 Python ingestion service       ingestion/main.py          runs on Windows, in a venv
   fetch → validate → flatten → JSON event
        │   key = city, value = JSON
        ▼
 Apache Kafka  topic "weather-data" (3 partitions)          Docker container
        │
        ▼
 Spark Structured Streaming     spark/weather_stream.py    Docker container
   parse (explicit schema) → clean → derive fields → data-quality rules
        │ foreachBatch (one micro-batch per TRIGGER_INTERVAL)
        ▼
 Snowflake  WEATHER_DB.WEATHER
   RAW_WEATHER        every message as received (audit / replay)
   WEATHER_REJECTED   records that failed DQ + the reason
   WEATHER_STAGE ─MERGE─► WEATHER_FACT   clean and deduplicated
   CITY_DIM           static reference data
   VW_* views         what Power BI reads
        │
        ▼
 Power BI Desktop (Import, or DirectQuery for live demos)
        │
        ▼
 4-page weather dashboard
```

## Why each component is here

| Component | Role in this POC | Why this one, and not something simpler |
|---|---|---|
| **OpenWeatherMap** | Data source: current weather for each city | Free tier (60 calls/min, 1M/month), no credit card, simple JSON. 7 cities every 5 min is ~2,000 calls/day. |
| **Python ingestion** | Calls the API, handles errors and retries, and turns nested JSON into a flat, versioned event | API calls are I/O work, not data processing. A small Python service is the right tool; Spark would be overkill for HTTP calls. |
| **Kafka** | Durable buffer between the producer and the consumer | **Decoupling.** If Spark or Snowflake is down, events wait in Kafka (7-day retention) and nothing is lost. Spark can **replay** from any offset. You can add consumers later without touching the producer. |
| **Spark Structured Streaming** | Parse, validate, clean, enrich, deduplicate, load | Shows real stream processing: explicit schemas, micro-batches, checkpoints (exactly-once bookkeeping), and `foreachBatch` sinks. The same code scales from 7 cities to 7 million events. |
| **Snowflake** | Warehouse: raw → fact, plus reporting views | SQL, `MERGE` for idempotent loads, `VARIANT`/JSON parsing for raw data, and a native Power BI connector. The warehouse auto-suspends, so an idle POC costs close to nothing. |
| **Power BI** | Dashboard | Native Snowflake connector, DAX measures, slicers. It's the tool most BI teams use. |
| **Docker** | Runs Kafka, Kafka UI and Spark | Kafka on native Windows is painful. PySpark streaming on Windows needs `winutils.exe`/`HADOOP_HOME`. Docker removes both problems. |

## Why transformations happen in Spark, not in Python

The Python producer does the **minimum**: it extracts fields and fails fast on responses with no temperature or timestamp. All business logic (casting, categories, heat index, DQ rules, dedup) lives in Spark because:

1. **One place for rules.** If you add a second producer (another API, a file drop), the rules still apply.
2. **Replayable.** If you change a rule, reset the Spark checkpoint and re-process the Kafka history. You can't re-run the API for yesterday's weather.
3. **Raw stays raw.** Kafka holds what the API said. RAW_WEATHER holds what Kafka delivered. Transformations are layered on top, never baked into the source.
4. **Scales.** Spark's engine is distributed. Python loops are not.

## Deliberately NOT used

| Technology | Why not |
|---|---|
| Airflow | Nothing to schedule. The producer loop and the streaming job both run continuously. |
| Schema Registry / Avro | Adds a service and serialization tooling. A `schema_version` field in JSON plus an explicit Spark schema is enough here. |
| dbt | 4 tables and 7 views. Plain SQL files are clearer at this size. |
| Kubernetes / cloud | One laptop, `docker compose`. |
| Great Expectations | 9 rules expressed as Spark columns, with reasons stored per record, cover the need without another framework. |
| ZooKeeper | Kafka 3.9 runs in KRaft mode (one container). |

## Event contract (Kafka message value)

```json
{
  "schema_version": 1,
  "source": "openweathermap",
  "city": "Delhi",
  "country": "IN",
  "city_id": 1273294,
  "api_location_name": "Delhi",
  "latitude": 28.6667,
  "longitude": 77.2167,
  "api_timestamp": "2026-09-28T08:50:00+00:00",
  "timezone_offset_seconds": 19800,
  "temperature": 31.05,
  "feels_like": 35.2,
  "temp_min": 31.05,
  "temp_max": 31.05,
  "humidity": 62,
  "pressure": 1004,
  "wind_speed": 2.57,
  "wind_direction": 90,
  "wind_gust": null,
  "cloudiness": 20,
  "visibility": 3000,
  "rain_1h": null,
  "snow_1h": null,
  "weather_condition": "Haze",
  "weather_description": "haze",
  "sunrise": "2026-09-28T00:40:00+00:00",
  "sunset": "2026-09-28T12:36:40+00:00",
  "ingestion_timestamp": "2026-09-28T08:52:18.500113+00:00"
}
```

* **Message key** = city name. Kafka hashes the key to choose a partition, so every Delhi event lands on the same partition and stays **in order**.
* `api_timestamp` = when the weather was *observed* (the API's `dt`). `ingestion_timestamp` = when *we* called the API. They differ, and the difference matters for deduplication.
* All timestamps are ISO-8601 UTC. Local time (IST) is derived in Spark from `timezone_offset_seconds`.

## Deduplication strategy (three layers)

OpenWeatherMap refreshes a city's current weather roughly every **10 minutes**. If you poll every 5 minutes you **will** receive the same observation twice. That's expected, not a bug.

| Layer | Mechanism | Catches |
|---|---|---|
| Kafka producer | `enable.idempotence=true` | Duplicate writes caused by producer network retries |
| Spark, within a micro-batch | `dropDuplicates(["event_id"])` where `event_id = SHA-256(upper(city) + api_timestamp)` | Two polls of the same observation in one batch |
| Snowflake, across batches | `MERGE INTO WEATHER_FACT ... WHEN NOT MATCHED THEN INSERT` on `EVENT_ID` | Same observation in different batches, **and** Spark re-running a batch after a crash |

RAW_WEATHER intentionally keeps everything, duplicates included. It is the audit log.

## Costs (important for a trial account)

* Every micro-batch that has data wakes the X-Small warehouse. Snowflake bills **at least 60 seconds** per resume.
* With a 5-minute poll and `AUTO_SUSPEND = 60`, that's about 12 resumes per hour, so roughly 0.2 credits/hour while the pipeline runs.
* **Stop the producer and Spark when you're not demoing.** The resource monitor in `01_setup.sql` suspends the warehouse at 5 credits/month as a safety net.
