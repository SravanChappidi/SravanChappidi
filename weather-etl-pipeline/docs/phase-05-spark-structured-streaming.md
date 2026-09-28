# Phase 5: Spark Structured Streaming

## 1. Objective
Run a PySpark Structured Streaming job that continuously reads `weather-data` from Kafka, parses the JSON with an **explicit schema**, and prints each micro-batch to the console.

## 2. Architecture position
```
Kafka ──► [Spark Structured Streaming] ──► console   (Snowflake from Phase 7)
              ▲ you are here
```

## 3. Prerequisites
* Kafka running with messages in `weather-data` (Phases 3–4)
* Docker Desktop has at least 4 GB RAM
* `.env` has `SINK_MODE=console`

## 4. Folder / file structure
```
spark/
├── weather_stream.py     entry point: readStream -> foreachBatch -> sink   ◄ this phase
├── schemas.py            explicit JSON schema                               ◄ this phase
├── transformations.py    parse / clean / derive  (parse part now)
├── data_quality.py       (Phase 8)
└── snowflake_writer.py   (Phase 7)
checkpoints/              created at runtime (git-ignored)
```

## 5. Commands
```powershell
cd docker
docker compose --profile spark up spark
#   First run downloads the Kafka + Snowflake connector jars (~60 MB) into a cached volume.
#   Ctrl+C stops it. Or run in the background:
docker compose --profile spark up -d spark
docker logs -f weather-spark

# In another window, feed it data:
python -m ingestion.main --mock           # or without --mock for real data
```
Spark UI while running: **http://localhost:4040**, then the **Structured Streaming** tab.

<details>
<summary>Alternative: run Spark natively on Windows (not recommended)</summary>

Needs Java 17, `pip install -r requirements-dev.txt`, **plus** `winutils.exe` and `hadoop.dll` for Hadoop 3.3 in `C:\hadoop\bin`, with `HADOOP_HOME=C:\hadoop` and `%HADOOP_HOME%\bin` on PATH (checkpoints fail without them). Then:
```powershell
$env:KAFKA_BOOTSTRAP_SERVERS="localhost:9092"; $env:CHECKPOINT_ROOT="./checkpoints"
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,net.snowflake:spark-snowflake_2.12:3.1.9 spark/weather_stream.py
```
</details>

## 6. Code

**Reading Kafka**: `weather_stream.py`
```python
kafka_stream = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)     # kafka:29092 inside Docker
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", STARTING_OFFSETS)            # "earliest" on first start only
    .option("maxOffsetsPerTrigger", 5000)
    .option("failOnDataLoss", "false")
    .load())

query = (kafka_stream.writeStream
    .foreachBatch(make_batch_processor(spark, sf_options))  # our code runs once per micro-batch
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(processingTime=TRIGGER_INTERVAL)               # "1 minute"
    .start())
```

**Explicit schema**: `schemas.py` (excerpt)
```python
WEATHER_EVENT_SCHEMA = StructType([
    StructField("city", StringType()),
    StructField("latitude", DoubleType()),
    StructField("api_timestamp", StringType()),   # cast to timestamp in clean()
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),        # cast to int in clean()
    ...
])
```

**Parsing**: `transformations.parse_kafka_messages()`
```python
.select(F.col("value").cast("string").alias("raw_payload"),
        F.col("partition").alias("kafka_partition"), F.col("offset").alias("kafka_offset"), ...)
.withColumn("is_valid_json", F.get_json_object("raw_payload", "$").isNotNull() & ...)
.withColumn("data", F.from_json("raw_payload", WEATHER_EVENT_SCHEMA))
.select("*", "data.*")
```

## 7. Explanation
* **Kafka source columns.** Spark sees each message as `key`, `value` (binary), `topic`, `partition`, `offset`, `timestamp`. We cast `value` to a string and parse it. We **keep** partition and offset for lineage.
* **Why an explicit schema?** Streaming can't infer schemas, and you don't want it to. If the producer renames a field, the column becomes NULL and a DQ rule flags it. Nothing silently drifts.
* **Why numbers as Double and timestamps as String?** `from_json` nulls a field when it doesn't fit the declared type. Reading `humidity` as Double means `65` and `65.0` both parse. We then round and cast deliberately in `clean()` (Phase 6).
* **Micro-batches.** Every `TRIGGER_INTERVAL` Spark checks Kafka for new offsets. If there are any, it runs **one batch** containing them. No new data means no batch and no cost.
* **Checkpoint** (`checkpoints/weather_console/`) stores which offsets were processed. Stop and restart the job and it resumes exactly where it stopped. Delete the folder and it re-reads from `STARTING_OFFSETS`.
* **`foreachBatch`** hands each micro-batch to our function as a normal DataFrame. That means any batch writer works (console now, Snowflake later), and we can run several writes per batch.
* **`spark.sql.shuffle.partitions = 2`**: the default is 200 tasks for a handful of rows. Reducing it is the single biggest speed-up for small data.
* **Why Docker?** The Linux container avoids the Windows `winutils` problem entirely and pins exact Spark/Java versions.

## 8. Expected output
```
weather-spark  | ... INFO weather_stream - Reading topic=weather-data from kafka:29092 | sink=console | trigger=1 minute | checkpoint=/app/checkpoints/weather_console
weather-spark  | +---------+--------------------+-----------+--------+----------+-----------------+ ...
weather-spark  | |city     |observation_ts_local|temperature|humidity|wind_speed|weather_condition| ...
weather-spark  | |Bengaluru|2026-09-28 14:20:00 |33.75      |37      |6.94      |Rain             | ...
weather-spark  | |Chennai  |2026-09-28 14:20:00 |28.38      |94      |8.81      |Clouds           | ...
weather-spark  | ...
weather-spark  | ... INFO weather_stream - BATCH 0 SUMMARY consumed=7 valid=7 rejected=0 in_batch_duplicates=0 duration=9.1s
```
The first batch takes about 10 s (JVM warm-up). Later batches take 1–3 s.

## 9. Testing steps
1. Start Spark and wait for `BATCH 0` (it processes existing messages).
2. Run one producer cycle. Within one trigger interval a new `BATCH 1` appears with `consumed=7`.
3. **Restart test:** `Ctrl+C`, start Spark again. It should **not** reprocess old messages (no batch until new data arrives). That's the checkpoint working.
4. **Replay test:** stop Spark, delete `checkpoints\weather_console`, start again. BATCH 0 now contains the whole topic history.
5. Spark UI → Structured Streaming: see input rate and batch durations.

## 10. Common errors and fixes

| Error | Fix |
|---|---|
| `Failed to find data source: kafka` | The `--packages` jar didn't load. Check the first lines of `docker logs weather-spark` for download errors (proxy/firewall). |
| `module not found: org.apache.spark#spark-sql-kafka...` | No internet from Docker, or Maven Central is blocked. Check Docker Desktop's proxy settings. |
| Stuck at `Connection to node -1 (localhost/127.0.0.1:9092) could not be established` | Spark is using `localhost:9092`. Inside Docker it must be `kafka:29092` (compose sets this). |
| `basedir must be absolute: ?/.ivy2/local` | Missing ivy dir config: keep `--conf spark.jars.ivy=/root/.ivy2` and `user: root` in compose. |
| Container killed / `exit code 137` | Out of memory. Give Docker Desktop more RAM. |
| Changed code but behaviour didn't change | `docker compose --profile spark up --force-recreate spark` |
| `The trigger interval is 60000 ms, but spent ...` | Just a warning on the slow first batch. Ignore it. |

## 11. Verify before moving on
- [ ] Spark prints a table for each producer cycle
- [ ] Restarting Spark doesn't reprocess old data; deleting the checkpoint does
- [ ] You can explain micro-batch, trigger, checkpoint and `foreachBatch`
