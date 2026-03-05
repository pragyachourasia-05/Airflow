from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def get_conn_and_cur():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn, conn.cursor()


@task
def train(train_input_table: str, train_view: str, forecast_function_name: str):
    conn, cur = get_conn_and_cur()
    try:
        cur.execute("BEGIN")
        cur.execute("USE DATABASE USER_DB_BISON;")
        cur.execute("USE SCHEMA SCHEMA_WEATHER_PREDICTION;")

        # View must contain DATE, TEMP_MAX, CITY for the forecast function
        create_view_sql = f"""
        CREATE OR REPLACE VIEW {train_view} AS
        SELECT
            DATE,
            TEMP_MAX,
            LOCATION_NAME AS CITY
        FROM {train_input_table}
        WHERE DATE >= DATEADD(day, -60, CURRENT_DATE());
        """

        create_model_sql = f"""
        CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
            INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
            SERIES_COLNAME => 'CITY',
            TIMESTAMP_COLNAME => 'DATE',
            TARGET_COLNAME => 'TEMP_MAX',
            CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
        );
        """

        cur.execute(create_view_sql)
        cur.execute(create_model_sql)
        cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")

        cur.execute("COMMIT")
    except Exception:
        cur.execute("ROLLBACK")
        raise
    finally:
        try:
            cur.close()
        finally:
            conn.close()


@task
def predict(forecast_function_name: str, forecast_table: str):
    conn, cur = get_conn_and_cur()
    try:
        cur.execute("BEGIN")
        cur.execute("USE DATABASE USER_DB_BISON;")
        cur.execute("USE SCHEMA SCHEMA_WEATHER_PREDICTION;")

        # 1) Prof-style forecast output table (SERIES, TS, FORECAST, LOWER_BOUND, UPPER_BOUND)
        make_prediction_sql = f"""
        BEGIN
            CALL {forecast_function_name}!FORECAST(
                FORECASTING_PERIODS => 7,
                CONFIG_OBJECT => {{'prediction_interval': 0.95}}
            );
            LET x := SQLID;
            CREATE OR REPLACE TABLE {forecast_table} AS
            SELECT * FROM TABLE(RESULT_SCAN(:x));
        END;
        """
        cur.execute(make_prediction_sql)

        # 2) Persist into WEATHER_FORECAST_DAILY for your union VIEW WEATHER_PREDICTION_FINAL
        cur.execute(f"""
        DELETE FROM WEATHER_FORECAST_DAILY
        WHERE run_ts::date = CURRENT_DATE()
          AND target_metric = 'temp_max'
          AND model_name = '{forecast_function_name}';
        """)

        cur.execute(f"""
        INSERT INTO WEATHER_FORECAST_DAILY
        (location_name, forecast_date, target_metric, forecast, lower_bound, upper_bound, model_name)
        SELECT
          REPLACE(series, '"', '') AS location_name,
          TO_DATE(ts) AS forecast_date,
          'temp_max' AS target_metric,
          forecast,
          lower_bound,
          upper_bound,
          '{forecast_function_name}' AS model_name
        FROM {forecast_table};
        """)

        cur.execute("COMMIT")
    except Exception:
        cur.execute("ROLLBACK")
        raise
    finally:
        try:
            cur.close()
        finally:
            conn.close()


with DAG(
    dag_id="TrainPredict",
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=["ML", "ELT", "weather"],
    schedule="30 2 * * *",  # 2:30 AM daily (after ETL)
) as dag:

    train_input_table = "WEATHER_PREDICTION_DAILY"
    train_view = "WEATHER_TRAIN_VIEW"
    forecast_function_name = "WEATHER_TEMPERATURE_MODEL"
    forecast_table = "WEATHER_FORECAST_TABLE"

    t1 = train(train_input_table, train_view, forecast_function_name)
    t2 = predict(forecast_function_name, forecast_table)

    t1 >> t2