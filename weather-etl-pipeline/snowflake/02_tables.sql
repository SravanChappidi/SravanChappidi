-- =====================================================================
-- 02_tables.sql  -  Tables
--
--   Kafka ──> RAW_WEATHER        every message exactly as received (audit / replay)
--        ├──> WEATHER_REJECTED   messages that failed data quality, with reasons
--        └──> WEATHER_STAGE ──MERGE──> WEATHER_FACT  (clean, de-duplicated)
--                                          │
--                                      CITY_DIM (static reference data)
--
-- All *_UTC timestamps are UTC. *_LOCAL timestamps are the city's local time (IST).
-- =====================================================================
USE ROLE WEATHER_ETL_ROLE;
USE WAREHOUSE WEATHER_WH;
USE SCHEMA WEATHER_DB.WEATHER;

-- ---------------------------------------------------------------------
-- RAW layer: append-only copy of every Kafka message (valid or not).
-- Kafka partition + offset uniquely identify a message -> full lineage.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_WEATHER (
    KAFKA_TOPIC        VARCHAR(100)   NOT NULL,
    KAFKA_PARTITION    INTEGER        NOT NULL,
    KAFKA_OFFSET       NUMBER(38,0)   NOT NULL,
    KAFKA_KEY          VARCHAR(100),
    KAFKA_TIMESTAMP    TIMESTAMP_NTZ,
    RAW_PAYLOAD        VARCHAR        NOT NULL,     -- original JSON text; parse with TRY_PARSE_JSON
    SPARK_BATCH_ID     NUMBER(38,0),
    LOADED_AT          TIMESTAMP_NTZ  DEFAULT SYSDATE()
)
COMMENT = 'Raw weather events exactly as read from Kafka (includes duplicates and bad records)';

-- ---------------------------------------------------------------------
-- Rejected records: routed here instead of silently disappearing.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS WEATHER_REJECTED (
    KAFKA_PARTITION    INTEGER,
    KAFKA_OFFSET       NUMBER(38,0),
    KAFKA_KEY          VARCHAR(100),
    CITY               VARCHAR(100),
    DQ_REASON          VARCHAR(1000)  NOT NULL,     -- e.g. 'INVALID_HUMIDITY,MISSING_WEATHER_CONDITION'
    RAW_PAYLOAD        VARCHAR,
    SPARK_BATCH_ID     NUMBER(38,0),
    REJECTED_AT        TIMESTAMP_NTZ  DEFAULT SYSDATE()
)
COMMENT = 'Weather events that failed data quality checks';

-- ---------------------------------------------------------------------
-- FACT: grain = ONE weather observation for ONE city at ONE API observation
-- timestamp (OBSERVATION_TS_UTC). EVENT_ID = SHA-256(city | observation time).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS WEATHER_FACT (
    EVENT_ID               VARCHAR(64)    NOT NULL,
    CITY                   VARCHAR(100)   NOT NULL,   -- joins to CITY_DIM.CITY
    COUNTRY                VARCHAR(2),
    API_CITY_ID            NUMBER(38,0),
    LATITUDE               NUMBER(8,4),
    LONGITUDE              NUMBER(8,4),
    OBSERVATION_TS_UTC     TIMESTAMP_NTZ  NOT NULL,
    OBSERVATION_TS_LOCAL   TIMESTAMP_NTZ  NOT NULL,
    OBSERVATION_DATE       DATE           NOT NULL,   -- local date
    OBSERVATION_HOUR       INTEGER        NOT NULL,   -- local hour 0-23
    TEMPERATURE            NUMBER(5,2)    NOT NULL,   -- °C
    FEELS_LIKE             NUMBER(5,2),
    TEMP_MIN               NUMBER(5,2),
    TEMP_MAX               NUMBER(5,2),
    HUMIDITY               INTEGER        NOT NULL,   -- %
    PRESSURE               INTEGER,                   -- hPa
    WIND_SPEED             NUMBER(6,2),               -- m/s
    WIND_DIRECTION         INTEGER,                   -- degrees
    WIND_GUST              NUMBER(6,2),
    CLOUDINESS             INTEGER,                   -- %
    VISIBILITY             INTEGER,                   -- metres (API max 10000)
    RAIN_1H                NUMBER(6,2),               -- mm in last hour (0 if none)
    SNOW_1H                NUMBER(6,2),
    WEATHER_CONDITION      VARCHAR(50)    NOT NULL,
    WEATHER_DESCRIPTION    VARCHAR(200),
    TEMPERATURE_CATEGORY   VARCHAR(20),
    WIND_CATEGORY          VARCHAR(20),
    IS_RAINY               BOOLEAN,
    HEAT_INDEX             NUMBER(5,2),
    HEAT_INDEX_CATEGORY    VARCHAR(20),
    SUNRISE_UTC            TIMESTAMP_NTZ,
    SUNSET_UTC             TIMESTAMP_NTZ,
    -- metadata / lineage
    INGESTION_TS_UTC       TIMESTAMP_NTZ,             -- when Python called the API
    PROCESSING_TS_UTC      TIMESTAMP_NTZ,             -- when Spark processed it
    KAFKA_PARTITION        INTEGER,
    KAFKA_OFFSET           NUMBER(38,0),
    SPARK_BATCH_ID         NUMBER(38,0),
    LOADED_AT              TIMESTAMP_NTZ  DEFAULT SYSDATE(),
    CONSTRAINT PK_WEATHER_FACT PRIMARY KEY (EVENT_ID)   -- informational in Snowflake; MERGE enforces it
)
COMMENT = 'One row per city per API observation timestamp (deduplicated)';

-- No CLUSTER BY: at a few thousand rows per week clustering adds cost, not speed.

-- Transient staging table: Spark truncates + reloads it every micro-batch, then
-- MERGEs it into the fact. Same columns as the fact (LOADED_AT just takes its default).
-- TRANSIENT = no Fail-safe storage cost, which is fine for throw-away staging data.
CREATE TRANSIENT TABLE IF NOT EXISTS WEATHER_STAGE LIKE WEATHER_FACT;

-- ---------------------------------------------------------------------
-- CITY_DIM: small static reference table. Adds attributes the API doesn't
-- give us (state, region, coastal) and a clean slicer table for Power BI.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CITY_DIM (
    CITY          VARCHAR(100)  NOT NULL,
    STATE         VARCHAR(100),
    REGION        VARCHAR(20),
    COUNTRY       VARCHAR(2),
    LATITUDE      NUMBER(8,4),
    LONGITUDE     NUMBER(8,4),
    IS_COASTAL    BOOLEAN,
    CONSTRAINT PK_CITY_DIM PRIMARY KEY (CITY)
)
COMMENT = 'City reference data';

MERGE INTO CITY_DIM t
USING (
    SELECT * FROM VALUES
        ('Delhi',     'Delhi',        'North', 'IN', 28.6667, 77.2167, FALSE),
        ('Mumbai',    'Maharashtra',  'West',  'IN', 19.0144, 72.8479, TRUE),
        ('Hyderabad', 'Telangana',    'South', 'IN', 17.3753, 78.4744, FALSE),
        ('Bengaluru', 'Karnataka',    'South', 'IN', 12.9762, 77.6033, FALSE),
        ('Chennai',   'Tamil Nadu',   'South', 'IN', 13.0878, 80.2785, TRUE),
        ('Kolkata',   'West Bengal',  'East',  'IN', 22.5697, 88.3697, FALSE),
        ('Pune',      'Maharashtra',  'West',  'IN', 18.5196, 73.8553, FALSE)
        AS v(CITY, STATE, REGION, COUNTRY, LATITUDE, LONGITUDE, IS_COASTAL)
) s
ON t.CITY = s.CITY
WHEN MATCHED THEN UPDATE SET STATE = s.STATE, REGION = s.REGION, COUNTRY = s.COUNTRY,
                             LATITUDE = s.LATITUDE, LONGITUDE = s.LONGITUDE, IS_COASTAL = s.IS_COASTAL
WHEN NOT MATCHED THEN INSERT (CITY, STATE, REGION, COUNTRY, LATITUDE, LONGITUDE, IS_COASTAL)
                      VALUES (s.CITY, s.STATE, s.REGION, s.COUNTRY, s.LATITUDE, s.LONGITUDE, s.IS_COASTAL);
