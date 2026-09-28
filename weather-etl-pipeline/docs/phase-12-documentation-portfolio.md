# Phase 12: Documentation and portfolio presentation

## 1. Objective
Turn the working pipeline into a portfolio piece: a clear README, screenshots, a 3–5 minute demo, and talking points for interviews.

## 2. Architecture position
Everything, explained to someone who has never seen it.

## 3. Prerequisites
Phase 11 completed, with measured timings and dashboard screenshots.

## 4. Folder / file structure
```
weather-etl-pipeline/
├── README.md                  entry point: what, why, architecture, how to run, results
└── docs/
    ├── 00-architecture.md
    ├── phase-01 ... phase-12
    └── images/
        ├── architecture.png   (optional: export from draw.io/Excalidraw)
        ├── kafka-ui.png
        ├── spark-logs.png
        ├── snowflake-fact.png
        ├── dashboard-overview.png
        ├── dashboard-trends.png
        ├── dashboard-conditions.png
        └── dashboard-city-detail.png
```

## 5. Steps
1. Take the screenshots listed above (Win + Shift + S).
2. Fill in the **Results** section of `README.md` with *your* numbers (days of data, rows, latency, credits used).
3. Record a demo video (Xbox Game Bar: Win + G, or OBS), following the script below. Upload it unlisted to YouTube or LinkedIn, and link it in the README.
4. Check that `.env` isn't committed: `git log --all --full-history -- .env` should print nothing. If it ever was, **rotate** the API key and Snowflake password.
5. Pin the repository on your GitHub profile.

## 6. Demo video script (3–5 min)
1. **(30 s) Problem + architecture:** "Weather for 7 Indian cities, near real time, from API to dashboard." Show the diagram.
2. **(45 s) Ingestion:** producer terminal with CYCLE SUMMARY lines; open `weather_api.py` and point at retry/timeout/validation.
3. **(30 s) Kafka:** Kafka UI: topic, 3 partitions, messages keyed by city.
4. **(60 s) Spark:** Spark logs with BATCH SUMMARY; show `transformations.py` and `data_quality.py`; run `produce_bad_events.py` and show REJECTED lines.
5. **(45 s) Snowflake:** `VW_PIPELINE_HEALTH`, `VW_LATEST_WEATHER`, the duplicate check returning 0 rows, `VW_DQ_REJECTION_SUMMARY`.
6. **(60 s) Power BI:** walk the 4 pages; click Refresh after a new cycle and show the latest time moving.
7. **(15 s) What you'd do next** (see below).

## 7. Explanation: talking points for interviews

**"Walk me through your pipeline."** Use the 8-step flow from Phase 11. Mention the numbers: 7 cities, 5-minute polling, about 1–2 minutes API-to-warehouse latency, N days of data, 0 duplicates.

**Design decisions you can defend:**
| Question | Your answer |
|---|---|
| Why Kafka between Python and Spark? | Decoupling and durability: Spark or Snowflake can be down without data loss; replay from offsets; more consumers can be added later. |
| How do you guarantee no duplicates? | Deterministic `event_id` (city + observation time); in-batch `dropDuplicates`; `MERGE ... WHEN NOT MATCHED` makes batch retries idempotent; idempotent producer. RAW keeps everything for audit. |
| What delivery semantics do you have? | Kafka → Spark: exactly-once *processing* bookkeeping via checkpoints. `foreachBatch` → Snowflake is at-least-once, made **effectively exactly-once** for the fact table by the idempotent MERGE. |
| What happens to bad data? | 9 rules, all failures listed per record, routed to `WEATHER_REJECTED` with raw payload and Kafka offset. Monitored via a view. |
| Why `foreachBatch` instead of a streaming sink? | The Snowflake connector is a batch writer, and I needed several writes plus a MERGE per micro-batch. Snowpipe Streaming would cut latency but adds components. It isn't needed for data that updates every 10 minutes. |
| Why Import mode in Power BI? | Small data, fast visuals, predictable warehouse cost. DirectQuery + auto page refresh is the option for live screens, with cost and a 30-minute Service minimum on shared capacity. |
| Why is `CITY_DIM` a natural key? | 7 static rows. A surrogate key would add a lookup with no benefit. I'd switch to surrogate keys if cities could be renamed or needed SCD history. |
| What would break at scale? | Stage table per batch is single-writer; many parallel streams would need per-stream stage tables or Snowpipe Streaming. `maxOffsetsPerTrigger` and partitions would need tuning. |

**Next steps (shows you know the gaps):**
* Schema Registry + Avro/Protobuf for enforced contracts
* Snowpipe Streaming / Kafka Connect Snowflake sink for second-level latency
* Orchestrated daily jobs (Airflow/dbt) for aggregates and SCD2 city attributes
* Alerts: e.g. a Snowflake alert when `MINUTES_SINCE_LAST_LOAD > 30`
* Containerise the producer and add CI running the unit tests
* Forecast API + comparison of forecast vs actual

**Resume bullets (edit numbers to yours):**
* Built a streaming weather data pipeline (Python, Kafka, PySpark Structured Streaming, Snowflake, Power BI) ingesting 7 cities every 5 minutes with ~1–2 min end-to-end latency.
* Implemented 9 record-level data-quality rules with reject routing and an idempotent Snowflake MERGE, giving zero duplicate facts across restarts and replays.
* Designed a raw → fact Snowflake model with reporting views and a 4-page Power BI dashboard (DAX latest-value, trend and freshness measures).

## 8. Expected output
A README that lets a stranger understand the project in 60 seconds and run it in 30 minutes, plus a demo link.

## 9. Testing steps
Ask a friend (or use a fresh clone in a new folder) to follow only the README. Every place they get stuck is a documentation bug. Fix it.

## 10. Common mistakes

| Mistake | Fix |
|---|---|
| Secrets in git history | Rotate the credentials; rewriting history alone isn't enough. |
| README is just commands | Lead with *what* and *why* plus the architecture image; commands come after. |
| Claiming "real-time dashboard" | Say "near-real-time pipeline; dashboard refreshes on demand or on schedule". Interviewers will ask. |
| No numbers | Add days of data, rows, latency, rejects, credits used. |

## 11. Final checklist
- [ ] README complete with screenshots and results
- [ ] Demo video linked
- [ ] No secrets in the repo
- [ ] You can answer every question in the talking-points table without notes
