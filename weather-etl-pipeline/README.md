# Dynamic Weather Data Pipeline: Kafka → Spark → Snowflake → Power BI

A near-real-time data engineering POC that polls live weather for 7 Indian cities, streams it through **Apache Kafka**, cleans and validates it with **PySpark Structured Streaming**, loads it idempotently into **Snowflake**, and visualises it in a 4-page **Power BI** dashboard. Built to run on one Windows laptop with Docker.

```
OpenWeatherMap ─► Python producer ─► Kafka ─► Spark Structured Streaming ─► Snowflake ─► Power BI
  (REST API)      retries, timeout,   topic    parse · clean · enrich ·     RAW → FACT     4-page
                  validation, logs    weather-  data quality · dedup        (MERGE),        dashboard
                                      data      foreachBatch               views
```

| | |
|---|---|
| **Cities** | Delhi, Mumbai, Hyderabad, Bengaluru, Chennai, Kolkata, Pune (configurable in `config/cities.json`) |
| **Polling** | every 5 min (`POLL_INTERVAL_SECONDS`) |
| **Latency** | API → Snowflake ≈ 1–2 min (1-minute Spark trigger) |
| **Guarantees** | no duplicate facts (deterministic `event_id` + idempotent MERGE); bad records kept with reasons |
| **Stack** | Python 3.11 · confluent-kafka · Kafka 3.9 (KRaft, Docker) · Spark 3.5 (Docker) · Snowflake · Power BI Desktop |

## Repository layout
```
weather-etl-pipeline/
├── .env.example               all settings; copy to .env (git-ignored)
├── requirements.txt           ingestion + tests
├── requirements-dev.txt       + pyspark for local Spark unit tests (optional)
├── config/cities.json         cities to poll
├── docker/docker-compose.yml  Kafka, Kafka UI, Spark
├── ingestion/                 API → Kafka
│   ├── config.py              settings from .env
│   ├── weather_api.py         HTTP client (timeout, retries) + event mapping
│   ├── kafka_producer.py      idempotent producer with delivery tracking
│   ├── mock_weather.py        offline fake API
│   └── main.py                polling loop (--once, --dry-run, --mock)
├── spark/                     Kafka → Snowflake
│   ├── weather_stream.py      streaming job (foreachBatch)
│   ├── schemas.py             explicit event schema
│   ├── transformations.py     parse, clean, derive
│   ├── data_quality.py        9 DQ rules, reject routing
│   └── snowflake_writer.py    RAW / REJECTED / STAGE → MERGE → FACT
├── snowflake/                 01_setup · 02_tables · 03_views · 04_validation_queries
├── powerbi/                   dax_measures.md · dashboard_design.md
├── scripts/                   consume_test.py · produce_bad_events.py
├── tests/                     unit tests (ingestion + Spark transformations)
└── docs/                      architecture + a step-by-step guide per phase
```

## Build it phase by phase

**Want to type or paste the code yourself in VS Code?** [docs/BUILD_GUIDE.md](docs/BUILD_GUIDE.md) has every file's full code, grouped by the phase where you create it.

| # | Phase | Guide |
|---|---|---|
| – | Architecture and design decisions | [docs/00-architecture.md](docs/00-architecture.md) |
| 1 | Project setup (Windows, venv, `.env`) | [phase-01](docs/phase-01-project-setup.md) |
| 2 | Weather API ingestion | [phase-02](docs/phase-02-weather-api-ingestion.md) |
| 3 | Kafka (Docker) and producer | [phase-03](docs/phase-03-kafka-producer.md) |
| 4 | Kafka topic and consumer validation | [phase-04](docs/phase-04-kafka-topic-consumer-validation.md) |
| 5 | Spark Structured Streaming | [phase-05](docs/phase-05-spark-structured-streaming.md) |
| 6 | Cleaning and transformation | [phase-06](docs/phase-06-cleaning-transformation.md) |
| 7 | Snowflake integration | [phase-07](docs/phase-07-snowflake-integration.md) |
| 8 | Data quality and validation | [phase-08](docs/phase-08-data-validation.md) |
| 9 | Power BI connection and model | [phase-09](docs/phase-09-powerbi-connection.md) |
| 10 | Dashboard development | [phase-10](docs/phase-10-dashboard-development.md) |
| 11 | End-to-end testing and monitoring | [phase-11](docs/phase-11-end-to-end-testing.md) |
| 12 | Documentation and portfolio | [phase-12](docs/phase-12-documentation-portfolio.md) |

Each guide has the same sections: objective · architecture position · prerequisites · files · commands · code · explanation · expected output · testing · common errors · checklist.

## Quick start (after Phase 1 setup)
```powershell
# 1. Kafka
cd docker; docker compose up -d
docker exec weather-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic weather-data --partitions 3 --replication-factor 1

# 2. Spark (SINK_MODE=console or snowflake in .env)
docker compose --profile spark up -d spark; docker logs -f weather-spark

# 3. Producer (new terminal, project root, venv active)
python -m ingestion.main            # add --mock to run without an API key
```
UIs: Kafka UI http://localhost:8080 · Spark UI http://localhost:4040

## Tests
```powershell
pytest -q tests/test_weather_api.py                                   # no extra dependencies
pip install -r requirements-dev.txt; pytest -q                        # + Spark tests (needs Java 17)
```

## Results
_Fill in after running it (Phase 12):_ days of data · fact rows · rejected rows · measured latency · Snowflake credits used · screenshots in `docs/images/`.
