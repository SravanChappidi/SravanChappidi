-- =====================================================================
-- 04_validation_queries.sql  -  Run these after the pipeline has loaded data.
-- Each query says what a "good" result looks like.
-- =====================================================================
USE ROLE WEATHER_ETL_ROLE;
USE WAREHOUSE WEATHER_WH;
USE SCHEMA WEATHER_DB.WEATHER;

-- 1. Row counts per layer. Expect RAW >= FACT + REJECTED
--    (RAW also holds repeats of the same observation, which the MERGE skips).
SELECT * FROM VW_PIPELINE_HEALTH;

-- 2. No duplicate business keys in the fact. Expect: 0 rows.
SELECT EVENT_ID, COUNT(*) FROM WEATHER_FACT GROUP BY EVENT_ID HAVING COUNT(*) > 1;

-- 3. Also no duplicate (city, observation time). Expect: 0 rows.
SELECT CITY, OBSERVATION_TS_UTC, COUNT(*)
FROM WEATHER_FACT GROUP BY 1, 2 HAVING COUNT(*) > 1;

-- 4. Every configured city is arriving, and recently. Expect 7 rows,
--    MINUTES_SINCE_OBSERVATION mostly < 30 while the pipeline runs.
SELECT CITY, OBSERVATION_TS_LOCAL, TEMPERATURE, MINUTES_SINCE_OBSERVATION
FROM VW_LATEST_WEATHER ORDER BY CITY;

-- 5. Nothing that should have been rejected made it into the fact. Expect: 0.
SELECT COUNT(*) AS BAD_ROWS_IN_FACT
FROM WEATHER_FACT
WHERE CITY IS NULL OR OBSERVATION_TS_UTC IS NULL
   OR TEMPERATURE NOT BETWEEN -60 AND 60
   OR HUMIDITY NOT BETWEEN 0 AND 100
   OR LATITUDE NOT BETWEEN -90 AND 90 OR LONGITUDE NOT BETWEEN -180 AND 180
   OR WEATHER_CONDITION IS NULL;

-- 6. Every fact city exists in CITY_DIM (Power BI relationship will work). Expect: 0 rows.
SELECT DISTINCT f.CITY FROM WEATHER_FACT f
LEFT JOIN CITY_DIM d ON d.CITY = f.CITY WHERE d.CITY IS NULL;

-- 7. Why were records rejected?
SELECT * FROM VW_DQ_REJECTION_SUMMARY ORDER BY REJECTED_DATE DESC, REJECTED_RECORDS DESC;
SELECT DQ_REASON, RAW_PAYLOAD, REJECTED_AT FROM WEATHER_REJECTED ORDER BY REJECTED_AT DESC LIMIT 20;

-- 8. Reconciliation: every Kafka message is accounted for exactly once
--    (either a fact row, a rejected row, or a duplicate of an existing fact row).
--    Expect UNACCOUNTED = 0.
WITH raw AS (
    SELECT DISTINCT KAFKA_PARTITION, KAFKA_OFFSET FROM RAW_WEATHER
),
accounted AS (
    SELECT KAFKA_PARTITION, KAFKA_OFFSET FROM WEATHER_REJECTED
    UNION
    SELECT r.KAFKA_PARTITION, r.KAFKA_OFFSET
    FROM VW_RAW_WEATHER_PARSED r
    JOIN WEATHER_FACT f
      ON f.CITY = INITCAP(TRIM(r.CITY)) AND f.OBSERVATION_TS_UTC = r.API_TIMESTAMP
)
SELECT
    (SELECT COUNT(*) FROM raw)                         AS RAW_MESSAGES,
    (SELECT COUNT(*) FROM accounted)                   AS ACCOUNTED,
    (SELECT COUNT(*) FROM raw) - (SELECT COUNT(*) FROM accounted) AS UNACCOUNTED;

-- 9. End-to-end latency: API call -> row in Snowflake (minutes).
SELECT CITY,
       ROUND(AVG(DATEDIFF('second', INGESTION_TS_UTC, LOADED_AT)) / 60, 1) AS AVG_LATENCY_MIN,
       MAX(DATEDIFF('second', INGESTION_TS_UTC, LOADED_AT)) / 60           AS MAX_LATENCY_MIN
FROM WEATHER_FACT
WHERE LOADED_AT > DATEADD('hour', -24, SYSDATE())
GROUP BY CITY ORDER BY CITY;

-- 10. Warehouse cost check: credits used by the POC warehouse in the last 7 days.
--     (ACCOUNT_USAGE has up to ~3h delay; needs ACCOUNTADMIN.)
-- USE ROLE ACCOUNTADMIN;
-- SELECT TO_DATE(START_TIME) AS DAY, SUM(CREDITS_USED) AS CREDITS
-- FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
-- WHERE WAREHOUSE_NAME = 'WEATHER_WH' AND START_TIME > DATEADD('day', -7, CURRENT_TIMESTAMP())
-- GROUP BY 1 ORDER BY 1;
