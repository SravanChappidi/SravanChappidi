# Phase 11: End-to-end testing, dynamic behaviour and monitoring

## 1. Objective
Run the whole pipeline together, prove that a new API reading flows all the way to the dashboard, test failure and recovery, and know where to look (logs, UIs, SQL) when something breaks.

## 2. Architecture position
All of it.

## 3. Prerequisites
Phases 1–10 complete.

## 4. Folder / file structure
No new files. Uses everything built so far.

## 5. Commands: full start-up sequence
```powershell
# Terminal 1: infrastructure + Spark
cd weather-etl-pipeline\docker
docker compose up -d                              # Kafka + Kafka UI
docker compose --profile spark up -d spark        # Spark (SINK_MODE=snowflake in .env)
docker logs -f weather-spark

# Terminal 2: producer
cd weather-etl-pipeline
.\.venv\Scripts\Activate.ps1
python -m ingestion.main

# Stop everything when done (saves Snowflake credits)
#   Ctrl+C in terminal 2
docker compose --profile spark stop spark
docker compose stop                               # "down -v" would also DELETE the Kafka data
```

## 6. The end-to-end demo script

| Step | What you do / show | Evidence |
|---|---|---|
| 1. Python calls the API | Producer terminal | `Fetched Delhi temp= 31.1C ...` |
| 2. Event generated | same | `CYCLE n SUMMARY ... published=7` |
| 3. Published to Kafka | Kafka UI → weather-data → Messages (newest first) | New message with this cycle's `ingestion_timestamp` |
| 4. Spark consumes | `docker logs weather-spark` | `BATCH n SUMMARY consumed=7` within ≤ 1 trigger interval |
| 5. Spark transforms | Spark UI (localhost:4040) → Structured Streaming | Batch duration, input rows |
| 6. Written to Snowflake | Snowsight: `SELECT * FROM VW_LATEST_WEATHER;` | `OBSERVATION_TS_LOCAL` = the new observation |
| 7. Power BI refreshes | Click **Refresh** (Import) or wait for automatic page refresh (DirectQuery) | |
| 8. Dashboard updates | Page 1 | `[Latest Observation Time]` and the city values change |

**Timing you can expect:** API → Kafka is under 1 s; Kafka → Snowflake takes up to `TRIGGER_INTERVAL` + about 5–10 s (1–1.5 min with the defaults); Snowflake → dashboard happens when you refresh.

> **Why doesn't the demo change every 5 minutes?** OpenWeatherMap updates each city about every 10 minutes. Some polls return the same observation, which the pipeline correctly deduplicates (`fact_inserted=0`). For a punchier live demo, run the producer with `--mock` (the observation time changes every 10 minutes, values change every poll) or temporarily set `POLL_INTERVAL_SECONDS=60` with mock data.

### Realistic limits of Power BI refresh
* **Import:** data changes only when a refresh runs. In Desktop that's the Refresh button. In the Power BI Service, scheduled refresh is limited to **8/day (Pro)** or **48/day (Premium Per User / capacity)**. It won't update "every few seconds".
* **DirectQuery + automatic page refresh:** visuals re-query Snowflake on a timer. Power BI Desktop allows short intervals. In the Service, **shared capacity has a minimum of 30 minutes**; shorter intervals need Premium/Fabric capacity with admin settings. Every refresh can resume the warehouse (cost).
* **Honest summary for this POC:** the *pipeline* is near real time (about 1–2 min from API to Snowflake). The *dashboard* is as fresh as its last refresh. That's the right trade-off for weather data that itself changes every ~10 minutes.

## 7. Failure and recovery tests

| Test | How | Expected behaviour |
|---|---|---|
| Spark down | `docker compose stop spark`, run the producer 2 cycles, start Spark | Events wait in Kafka; on restart Spark processes both cycles in one batch (checkpoint), no loss |
| Snowflake unreachable | Set a wrong `SNOWFLAKE_PASSWORD`, recreate Spark | Batch fails, the query stops with an auth error in the logs. **Offsets aren't committed.** Fix the password, restart: the same batch is retried, and MERGE prevents duplicates |
| Kafka down | `docker compose stop kafka` while the producer runs | Producer logs `Delivery failed ... Message timed out` after 30 s, `delivery_failed=7`; resumes on the next cycle after `docker compose start kafka` |
| API failure | Bad API key | `api_failures=7`, loop continues; fix key without restarting Spark |
| Bad data | `python scripts/produce_bad_events.py` | 7 rejects with reasons, valid data unaffected |
| Replay / idempotency | Delete `checkpoints\weather_snowflake`, restart Spark | RAW grows, FACT count unchanged |

> The producer doesn't buffer events locally when Kafka is down longer than `message.timeout.ms`; those cycles are lost (logged as `delivery_failed`). For weather data, missing a 5-minute reading is acceptable. In a system where every event matters, you'd add a local spool file.

## 8. Monitoring: what is tracked and where

| Metric | Where |
|---|---|
| API request success/failure | Producer log: `CYCLE n SUMMARY ... fetched=7 api_failures=0` and an `API failure:` line per failure |
| Records fetched | `fetched=` |
| Kafka messages produced | `published=`, confirmed by the broker: `delivered=`, `delivery_failed=` |
| Kafka messages consumed | Spark log: `BATCH n SUMMARY consumed=` |
| Valid / rejected records | `valid=`, `rejected=`, plus a `REJECTED ... reasons=` line per bad record |
| Duplicates | `in_batch_duplicates=`, and `staged` vs `fact_inserted` for cross-batch duplicates |
| Records written to Snowflake | `raw_written=`, `rejected_written=`, `staged=`, `fact_inserted=` |
| Freshness | `VW_PIPELINE_HEALTH.MINUTES_SINCE_LAST_LOAD`; `[Data Status]` card in Power BI |
| Throughput/latency | Spark UI → Structured Streaming; query 9 in `04_validation_queries.sql` |
| Snowflake cost | Snowsight → Admin → Cost management; query 10 in `04_validation_queries.sql` |

Example of a healthy log pair:
```
2026-09-28 09:05:00 INFO ingestion - CYCLE 14 SUMMARY cities=7 fetched=7 api_failures=0 invalid=0 published=7 delivered=7 delivery_failed=0 pending=0 duration=1.4s
2026-09-28 09:06:00 INFO weather_stream - BATCH 14 SUMMARY consumed=7 valid=7 rejected=0 in_batch_duplicates=0 raw_written=7 rejected_written=0 staged=7 fact_inserted=7 duration=5.9s
```
An unhealthy one, and what it tells you:
```
... ERROR ingestion - API failure: Pune: HTTP 502: ...           -> API side, retried and gave up. Other cities OK.
... CYCLE 15 SUMMARY ... fetched=6 api_failures=1 ... delivered=6
... BATCH 15 SUMMARY consumed=6 valid=6 ... fact_inserted=0      -> the API hadn't refreshed yet (normal)
```

To keep a log file as well as the console:
```powershell
mkdir logs -Force    # git-ignored
python -m ingestion.main 2>&1 | Tee-Object -FilePath logs\ingestion.log -Append
docker logs weather-spark > logs\spark.log 2>&1
```

## 9. Testing steps
Run the demo script (section 6) once, start to finish, **timing each step**, then run every failure test (section 7). Write down the actual timings; they go into your README.

## 10. Common errors and fixes

| Symptom | Check first |
|---|---|
| Dashboard not updating | 1) Producer still running? 2) Spark `BATCH` lines appearing? 3) `VW_PIPELINE_HEALTH.MINUTES_SINCE_LAST_LOAD` 4) Did you click Refresh? |
| Spark query terminated | `docker logs weather-spark`. The exception is at the bottom. Fix, then `docker compose --profile spark up -d spark`. |
| `fact_inserted=0` for every batch | Normal if the API hasn't updated. If it lasts > 30 min, compare `api_timestamp` in Kafka UI; it may be stuck on the API side. |
| Laptop slow | Stop Kafka UI (`docker compose stop kafka-ui`); lower Spark to `local[1]`. |

## 11. Verify before moving on
- [ ] Demo script runs end to end, with measured latencies
- [ ] Every failure test behaves as described
- [ ] You can explain Power BI refresh limits honestly
