USE DATABASE USER_DB_BISON;
USE SCHEMA SCHEMA_WEATHER_PREDICTION;

CREATE OR REPLACE TABLE WEATHER_LOCATIONS (
  location_name STRING PRIMARY KEY,
  latitude FLOAT,
  longitude FLOAT
);

INSERT OVERWRITE INTO WEATHER_LOCATIONS VALUES
('San Jose, CA', 37.3382, -121.8863),
('Cupertino, CA', 37.3229, -122.0322);
--------------------------------------------------
##Raw/History table (required schema)#

CREATE OR REPLACE TABLE WEATHER_PREDICTION_DAILY (
  location_name STRING,
  latitude FLOAT,
  longitude FLOAT,
  date DATE,
  temp_max NUMBER(5,1),
  temp_min NUMBER(5,1),
  temp_mean NUMBER(5,1),
  ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT pk_weather_raw PRIMARY KEY (location_name, date)
);
------------------------------------------------------------
-- 3) Stage table for Airflow loads
CREATE OR REPLACE TABLE WEATHER_STAGE (
  location_name STRING,
  latitude FLOAT,
  longitude FLOAT,
  date DATE,
  temp_max NUMBER(5,1),
  temp_min NUMBER(5,1),
  temp_mean NUMBER(5,1),
  load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
---------------------------------------------------------------------
-- Merge procedure: Stage -> Raw (Airflow will call this daily)
CREATE OR REPLACE PROCEDURE MERGE_WEATHER()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  MERGE INTO WEATHER_PREDICTION_DAILY t
  USING (
    SELECT location_name, latitude, longitude, date, temp_max, temp_min, temp_mean
    FROM WEATHER_STAGE
  ) s
  ON t.location_name = s.location_name AND t.date = s.date
  WHEN MATCHED THEN UPDATE SET
    t.latitude = s.latitude,
    t.longitude = s.longitude,
    t.temp_max = s.temp_max,
    t.temp_min = s.temp_min,
    t.temp_mean = s.temp_mean,
    t.ingestion_ts = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT
    (location_name, latitude, longitude, date, temp_max, temp_min, temp_mean)
  VALUES
    (s.location_name, s.latitude, s.longitude, s.date, s.temp_max, s.temp_min, s.temp_mean);

  TRUNCATE TABLE WEATHER_STAGE;

  RETURN 'MERGE completed and stage truncated.';
END;
$$;
---------------------------------------------
##4) Forecast output table

CREATE OR REPLACE TABLE WEATHER_FORECAST_DAILY (
  location_name STRING,
  forecast_date DATE,
  target_metric STRING,
  forecast NUMBER(10,3),
  lower_bound NUMBER(10,3),
  upper_bound NUMBER(10,3),
  model_name STRING,
  run_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
------------------------------------------------------
###---5) Training view (

CREATE OR REPLACE VIEW WEATHER_TRAIN_VIEW AS
SELECT
  location_name AS series,
  date AS ds,
  temp_max AS y
FROM WEATHER_PREDICTION_DAILY
WHERE date >= DATEADD(day, -60, CURRENT_DATE());

-- 6) Create/replace multi-series forecast model

CREATE OR REPLACE SNOWFLAKE.ML.FORECAST WEATHER_TEMPERATURE_MODEL(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'WEATHER_TRAIN_VIEW'),
  SERIES_COLNAME => 'SERIES',
  TIMESTAMP_COLNAME => 'DS',
  TARGET_COLNAME => 'Y'
);

##

-- 7) Procedure to run forecast + insert into forecast table
CREATE OR REPLACE PROCEDURE RUN_WEATHER_FORECAST()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  DELETE FROM WEATHER_FORECAST_DAILY
  WHERE run_ts::date = CURRENT_DATE()
    AND target_metric = 'temp_max'
    AND model_name = 'WEATHER_TEMPERATURE_MODEL';

  INSERT INTO WEATHER_FORECAST_DAILY
  (location_name, forecast_date, target_metric, forecast, lower_bound, upper_bound, model_name)
  SELECT
    series::string AS location_name,
    TO_DATE(ts) AS forecast_date,
    'temp_max' AS target_metric,
    forecast,
    lower_bound,
    upper_bound,
    'WEATHER_TEMPERATURE_MODEL' AS model_name
  FROM TABLE(
    WEATHER_TEMPERATURE_MODEL!FORECAST(
      FORECASTING_PERIODS => 7,
      CONFIG_OBJECT => {'prediction_interval': 0.95}
    )
  );

  RETURN 'Forecast inserted into WEATHER_FORECAST_DAILY.';
END;
$$;


-- 8) Final union view (history + forecast)
CREATE OR REPLACE VIEW WEATHER_PREDICTION_FINAL AS
SELECT
  location_name AS CITY,
  date AS DATE,
  temp_max AS ACTUAL,
  NULL::NUMBER AS FORECAST,
  NULL::NUMBER AS LOWER_BOUND,
  NULL::NUMBER AS UPPER_BOUND
FROM WEATHER_PREDICTION_DAILY

UNION ALL

SELECT
  location_name AS CITY,
  forecast_date AS DATE,
  NULL::NUMBER AS ACTUAL,
  forecast AS FORECAST,
  lower_bound AS LOWER_BOUND,
  upper_bound AS UPPER_BOUND
FROM WEATHER_FORECAST_DAILY;

SELECT *
FROM WEATHER_PREDICTION_FINAL
ORDER BY DATE DESC, CITY;

SELECT COUNT(*) FROM WEATHER_PREDICTION_DAILY;
SELECT COUNT(*) FROM WEATHER_FORECAST_DAILY;

SELECT COUNT(*) FROM WEATHER_PREDICTION_DAILY;
SELECT COUNT(*) FROM WEATHER_FORECAST_TABLE;  
SELECT COUNT(*) FROM WEATHER_FORECAST_DAILY;   


SELECT
  CITY,
  DATE,
  ACTUAL,
  FORECAST,
  LOWER_BOUND,
  UPPER_BOUND
FROM WEATHER_PREDICTION_FINAL
WHERE ACTUAL IS NULL
ORDER BY DATE, CITY;

#--RUN CHECK
