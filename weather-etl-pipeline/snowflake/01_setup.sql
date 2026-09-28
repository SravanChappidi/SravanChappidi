-- =====================================================================
-- 01_setup.sql  -  Warehouse, database, schema, role and service user
-- Run as ACCOUNTADMIN (a trial account gives you this) in a Snowsight worksheet.
-- =====================================================================
USE ROLE ACCOUNTADMIN;

-- X-Small is plenty: we load a handful of rows per micro-batch.
-- AUTO_SUSPEND = 60 stops credit burn as soon as the pipeline goes quiet.
CREATE WAREHOUSE IF NOT EXISTS WEATHER_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Weather ETL POC - Spark loads + Power BI queries';

CREATE DATABASE IF NOT EXISTS WEATHER_DB COMMENT = 'Weather ETL POC';
CREATE SCHEMA IF NOT EXISTS WEATHER_DB.WEATHER COMMENT = 'Raw, fact, dimension and reporting views';

-- One role for the pipeline (writes) and a read-only role for Power BI.
CREATE ROLE IF NOT EXISTS WEATHER_ETL_ROLE;
CREATE ROLE IF NOT EXISTS WEATHER_BI_ROLE;

GRANT USAGE ON WAREHOUSE WEATHER_WH TO ROLE WEATHER_ETL_ROLE;
GRANT USAGE ON WAREHOUSE WEATHER_WH TO ROLE WEATHER_BI_ROLE;
GRANT USAGE ON DATABASE WEATHER_DB TO ROLE WEATHER_ETL_ROLE;
GRANT USAGE ON DATABASE WEATHER_DB TO ROLE WEATHER_BI_ROLE;
GRANT ALL ON SCHEMA WEATHER_DB.WEATHER TO ROLE WEATHER_ETL_ROLE;   -- needs CREATE STAGE for the Spark connector
GRANT USAGE ON SCHEMA WEATHER_DB.WEATHER TO ROLE WEATHER_BI_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA WEATHER_DB.WEATHER TO ROLE WEATHER_BI_ROLE;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA WEATHER_DB.WEATHER TO ROLE WEATHER_BI_ROLE;
GRANT ROLE WEATHER_ETL_ROLE TO ROLE SYSADMIN;
GRANT ROLE WEATHER_BI_ROLE  TO ROLE SYSADMIN;

-- Service user for Spark. Replace the password (or see the PAT option below).
-- TYPE = LEGACY_SERVICE allows password login for non-human users on accounts
-- where that is still permitted. Never put this password in code - only in .env.
CREATE USER IF NOT EXISTS WEATHER_ETL_USER
  PASSWORD = 'Change-Me-Str0ng-Passw0rd!'
  TYPE = LEGACY_SERVICE
  DEFAULT_ROLE = WEATHER_ETL_ROLE
  DEFAULT_WAREHOUSE = WEATHER_WH
  DEFAULT_NAMESPACE = WEATHER_DB.WEATHER
  COMMENT = 'Spark Structured Streaming loader';
GRANT ROLE WEATHER_ETL_ROLE TO USER WEATHER_ETL_USER;

-- Let YOUR login use the BI role from Power BI (replace with your username).
-- GRANT ROLE WEATHER_BI_ROLE TO USER <YOUR_SNOWFLAKE_LOGIN>;

-- ---------------------------------------------------------------------
-- OPTION: if your account rejects password logins, create a Programmatic
-- Access Token and put the token string in SNOWFLAKE_PASSWORD in .env.
-- (PATs require a network policy unless an authentication policy relaxes it;
--  see Snowflake docs "Using programmatic access tokens".)
-- ALTER USER WEATHER_ETL_USER SET TYPE = SERVICE;
-- ALTER USER WEATHER_ETL_USER ADD PROGRAMMATIC ACCESS TOKEN WEATHER_ETL_PAT
--   ROLE_RESTRICTION = 'WEATHER_ETL_ROLE' DAYS_TO_EXPIRY = 90;
-- ---------------------------------------------------------------------

-- Optional safety net for a trial account: cap spend at 5 credits / month.
CREATE RESOURCE MONITOR IF NOT EXISTS WEATHER_POC_MONITOR
  WITH CREDIT_QUOTA = 5 FREQUENCY = MONTHLY START_TIMESTAMP = IMMEDIATELY
  TRIGGERS ON 80 PERCENT DO NOTIFY
           ON 100 PERCENT DO SUSPEND;
ALTER WAREHOUSE WEATHER_WH SET RESOURCE_MONITOR = WEATHER_POC_MONITOR;
