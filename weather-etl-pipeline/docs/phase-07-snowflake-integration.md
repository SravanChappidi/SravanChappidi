# Phase 7: Snowflake integration

## 1. Objective
Create the Snowflake objects (warehouse, database, schema, roles, tables, views) and switch Spark to write every micro-batch into Snowflake **idempotently**: no duplicate fact rows, even after restarts.

## 2. Architecture position
```
Spark ──foreachBatch──► Snowflake: RAW_WEATHER / WEATHER_REJECTED / WEATHER_STAGE ─MERGE─► WEATHER_FACT
```

## 3. Prerequisites
* Snowflake trial account; you can log in to Snowsight
* Your **account identifier**: Snowsight → bottom-left account menu → **Connect a tool to Snowflake**, or run `SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME();`. It looks like `abcdefg-xy12345`.
* Phases 5–6 working in console mode

## 4. Folder / file structure
```
snowflake/
├── 01_setup.sql              warehouse, DB, schema, roles, service user, resource monitor
├── 02_tables.sql             RAW_WEATHER, WEATHER_REJECTED, WEATHER_FACT, WEATHER_STAGE, CITY_DIM
├── 03_views.sql              reporting views for Power BI
└── 04_validation_queries.sql (Phase 8)
spark/snowflake_writer.py     foreachBatch -> Snowflake                  ◄ this phase
```

## 5. Commands
1. Snowsight → **Projects → Worksheets → +**. Paste and run **all** of `01_setup.sql` (Run All = Ctrl+Shift+Enter). **Change the password first.**
2. Run `02_tables.sql`, then `03_views.sql`.
3. Fill in `.env`:
   ```ini
   SNOWFLAKE_ACCOUNT=abcdefg-xy12345
   SNOWFLAKE_USER=WEATHER_ETL_USER
   SNOWFLAKE_PASSWORD=<the password you set>
   SNOWFLAKE_DATABASE=WEATHER_DB
   SNOWFLAKE_SCHEMA=WEATHER
   SNOWFLAKE_WAREHOUSE=WEATHER_WH
   SNOWFLAKE_ROLE=WEATHER_ETL_ROLE
   SINK_MODE=snowflake
   ```
4. Restart Spark:
   ```powershell
   cd docker
   docker compose --profile spark up --force-recreate spark
   ```
   The `snowflake` sink uses its own checkpoint (`checkpoints/weather_snowflake`), so on first start it loads the **whole topic history** (`STARTING_OFFSETS=earliest`).

## 6. Code

**Table design (grain)**: `02_tables.sql`
```sql
-- grain = ONE weather observation for ONE city at ONE API observation timestamp
CREATE TABLE IF NOT EXISTS WEATHER_FACT (
    EVENT_ID             VARCHAR(64)   NOT NULL,   -- SHA-256(city | observation time)
    CITY                 VARCHAR(100)  NOT NULL,   -- -> CITY_DIM.CITY
    OBSERVATION_TS_UTC   TIMESTAMP_NTZ NOT NULL,
    OBSERVATION_TS_LOCAL TIMESTAMP_NTZ NOT NULL,
    OBSERVATION_DATE     DATE          NOT NULL,
    TEMPERATURE          NUMBER(5,2)   NOT NULL,
    ...
    INGESTION_TS_UTC     TIMESTAMP_NTZ,            -- metadata / lineage
    PROCESSING_TS_UTC    TIMESTAMP_NTZ,
    KAFKA_PARTITION      INTEGER,
    KAFKA_OFFSET         NUMBER(38,0),
    SPARK_BATCH_ID       NUMBER(38,0),
    LOADED_AT            TIMESTAMP_NTZ DEFAULT SYSDATE(),
    CONSTRAINT PK_WEATHER_FACT PRIMARY KEY (EVENT_ID)
);
```

**Writing one micro-batch**: `snowflake_writer.py`
```python
_write(_select_as(all_df, RAW_COLUMNS, batch_id), "RAW_WEATHER", options)              # 1. raw, append
_write(_select_as(rejected_df, REJECTED_COLUMNS, batch_id), "WEATHER_REJECTED", options)  # 2. rejects
_write(_select_as(valid_df, FACT_COLUMNS, batch_id), "WEATHER_STAGE", options,          # 3. stage, truncate+load
       mode="overwrite", truncate_table="on", usestagingtable="off")
counts["fact_inserted"] = _run_query(spark, options, _merge_sql())                      # 4. MERGE
```
```sql
MERGE INTO WEATHER_FACT t
USING (SELECT * FROM WEATHER_STAGE
       QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_ID ORDER BY INGESTION_TS_UTC DESC) = 1) s
ON t.EVENT_ID = s.EVENT_ID
WHEN NOT MATCHED THEN INSERT (...) VALUES (...)
```
The MERGE runs through the connector's JVM helper `net.snowflake.spark.snowflake.Utils.runQuery(options, sql)`.

## 7. Explanation

**Data model**
* **RAW_WEATHER** is an append-only copy of every Kafka message, including bad and duplicate ones. It's your audit trail and replay source. `(KAFKA_PARTITION, KAFKA_OFFSET)` identifies each message. The raw JSON is stored as text; `VW_RAW_WEATHER_PARSED` parses it with `TRY_PARSE_JSON`.
* **WEATHER_REJECTED** holds the rows that failed DQ, with a `DQ_REASON`. Nothing disappears silently.
* **WEATHER_FACT** holds the clean, deduplicated observations. **Grain: one row = one observation for one city at one API observation timestamp.** `EVENT_ID` is the natural business key, hashed.
* **CITY_DIM** is worth having: it adds state/region/coastal attributes the API doesn't provide, and gives Power BI a clean slicer table. `CITY` is its key. A surrogate integer key would add a lookup step with no benefit at 7 rows.
* **Views** are what Power BI reads, so the tables can evolve without breaking the report.
* Snowflake **doesn't enforce** PRIMARY KEY constraints. They document intent (and Power BI can read them). The MERGE is what actually enforces uniqueness.

**Why `foreachBatch` + micro-batch loading (and not "true streaming")?**
The Spark–Snowflake connector is a **batch** writer: each write stages files and runs `COPY INTO`. `foreachBatch` lets us use it from a stream, and lets us do *several* writes plus a MERGE per batch, which a plain streaming sink can't. Snowpipe Streaming or Kafka Connect → Snowflake would give lower latency but add another component and more setup. With a 5-minute API update cycle, a 1-minute trigger gives about a 1–2 minute end-to-end latency, which is more than enough here.

**Why stage + MERGE instead of appending to the fact?**
Three sources of duplicates: (1) polling faster than the API updates, (2) the same observation arriving in different batches, and (3) **Spark retrying a batch** after a crash (`foreachBatch` is *at-least-once*). A MERGE keyed on `EVENT_ID` that only inserts when not matched is **idempotent**: running it twice gives the same result. That makes the whole pipeline effectively exactly-once for the fact table. RAW can get the same message twice if a batch retries. That's acceptable for an audit table, and the `(partition, offset)` pair lets you spot it.

**Why WEATHER_STAGE is TRANSIENT:** it's truncated every batch, so there's no need for Fail-safe (7 extra days of storage).

**`column_mapping=name`**: DataFrame columns are matched to table columns by name, so column order doesn't matter, and table columns missing from the DataFrame (`LOADED_AT`) get their `DEFAULT`.

**Authentication note.** Snowflake is phasing out password-only logins. The setup script creates the loader as `TYPE = LEGACY_SERVICE` so a password works where your account still allows it. If you get `Failed to authenticate: ... password ... not allowed`, create a **Programmatic Access Token** (commented block in `01_setup.sql`) and put the token in `SNOWFLAKE_PASSWORD`. Key-pair auth is the long-term production option.

## 8. Expected output
Spark logs:
```
... INFO weather_stream - BATCH 3 SUMMARY consumed=7 valid=7 rejected=0 in_batch_duplicates=0 raw_written=7 rejected_written=0 staged=7 fact_inserted=7 duration=6.8s
... INFO weather_stream - BATCH 4 SUMMARY consumed=7 valid=7 rejected=0 in_batch_duplicates=0 raw_written=7 rejected_written=0 staged=7 fact_inserted=0 duration=5.2s
```
Batch 4 shows `fact_inserted=0`: the API hadn't updated yet (same observation times), so the MERGE correctly skipped all 7.

Snowflake:
```sql
SELECT * FROM VW_PIPELINE_HEALTH;
-- RAW_MESSAGES=14  FACT_ROWS=7  REJECTED_ROWS=0  CITIES_LOADED=7  MINUTES_SINCE_LAST_LOAD=1
```

## 9. Testing steps
1. `SELECT * FROM WEATHER_FACT ORDER BY LOADED_AT DESC LIMIT 20;` shows rows arriving.
2. `SELECT * FROM VW_LATEST_WEATHER;` shows 7 rows, one per city.
3. **Idempotency test:** stop Spark, delete `checkpoints\weather_snowflake`, start again. Spark re-sends the whole topic. RAW_WEATHER doubles, but **WEATHER_FACT's count doesn't change** (`fact_inserted=0`).
4. Snowsight → **Monitoring → Query History**, filter on user `WEATHER_ETL_USER`: you'll see `COPY INTO` and `MERGE` per batch.

## 10. Common errors and fixes

| Error | Fix |
|---|---|
| `Incorrect username or password was specified` | Check `.env`. If the password has `#` or quotes, wrap it: `SNOWFLAKE_PASSWORD="p@ss#1"`. |
| `password ... not allowed` / MFA required | Use a PAT (see `01_setup.sql`), or check the user has `TYPE=LEGACY_SERVICE`. |
| `JDBC driver encountered communication error` / 403 | Wrong `SNOWFLAKE_ACCOUNT` format. Use `org-account`, no `https://`, no `.snowflakecomputing.com`. |
| `Object 'WEATHER_STAGE' does not exist or not authorized` | `02_tables.sql` wasn't run, or `SNOWFLAKE_ROLE` isn't `WEATHER_ETL_ROLE`. |
| `Insufficient privileges to operate on schema` / `CREATE STAGE` | The connector creates a temporary internal stage. `01_setup.sql` grants `ALL ON SCHEMA` for this. Re-run it. |
| `NULL result in a non-nullable column` | A value slipped past DQ. Check which column, and add or adjust a rule in `data_quality.py`. |
| `Missing Snowflake settings in .env: ...` | Spark fails fast at start. Fill in the listed variables. |
| Warehouse credits climbing | `AUTO_SUSPEND=60`? Stop the producer and Spark when you're not using them. |

## 11. Verify before moving on
- [ ] All three SQL scripts ran without errors
- [ ] `WEATHER_FACT` receives 7 rows per new API observation
- [ ] The idempotency test leaves the fact count unchanged
- [ ] You can state the fact table's grain and explain why the MERGE makes loads idempotent
