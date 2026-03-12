from __future__ import annotations

from datetime import datetime, timedelta
import os
import csv
import requests

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


SNOWFLAKE_CONN_ID = "snowflake_conn"

# ---------------- Helper functions ----------------

def get_logical_date() -> str:
    """Return the DAG run logical date in YYYY-MM-DD."""
    context = get_current_context()
    return str(context["logical_date"])[:10]


def return_snowflake_cursor(con_id: str):
    """Return Snowflake cursor using Airflow Connection ID."""
    hook = SnowflakeHook(snowflake_conn_id=con_id)
    conn = hook.get_conn()
    return conn.cursor()


def fetch_weather_json(start_date: str, end_date: str, latitude: float, longitude: float) -> dict:
    """Fetch daily historical weather from Open-Meteo archive endpoint."""
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "weathercode",
        ],
        "timezone": "America/Los_Angeles",
    }

    resp = requests.get(url, params=params, timeout=60)
    print("Status Code:", resp.status_code)
    resp.raise_for_status()
    return resp.json()


def save_weather_csv(city: str, latitude: float, longitude: float, start_date: str, end_date: str, file_path: str) -> None:
    """Save weather data to CSV in /tmp for the run date range."""
    data = fetch_weather_json(start_date, end_date, latitude, longitude)
    daily = data.get("daily", {})

    dates = daily.get("time", [])
    tmax = daily.get("temperature_2m_max", [])
    tmin = daily.get("temperature_2m_min", [])
    precip = daily.get("precipitation_sum", [])
    wcode = daily.get("weathercode", [])

    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "temp_max", "temp_min", "precipitation", "weather_code", "city"])
        for i in range(len(dates)):
            writer.writerow([
                dates[i],
                tmax[i] if i < len(tmax) else "",
                tmin[i] if i < len(tmin) else "",
                precip[i] if i < len(precip) else "",
                wcode[i] if i < len(wcode) else "",
                city,
            ])


def populate_table_via_stage(cur, database: str, schema: str, table: str, file_path: str) -> None:
    """Populate a table with data from local CSV using Snowflake PUT + COPY INTO."""
    stage_name = f"TEMP_STAGE_{table}"
    file_name = os.path.basename(file_path)

    cur.execute(f"USE SCHEMA {database}.{schema};")
    cur.execute(f"CREATE TEMPORARY STAGE {stage_name};")
    cur.execute(f"PUT file://{file_path} @{stage_name};")

    copy_query = f"""
        COPY INTO {schema}.{table}
        FROM @{stage_name}/{file_name}
        FILE_FORMAT = (
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
            EMPTY_FIELD_AS_NULL = TRUE
        )
    """
    cur.execute(copy_query)


# ---------------- Tasks ----------------

@task
def get_config() -> dict:
    """Reads Airflow Variables (same as HW5)."""
    lat = float(Variable.get("latitude"))
    lon = float(Variable.get("longitude"))

    return {
        "city": "Los_Angeles",
        "latitude": lat,
        "longitude": lon,
        "database": "USER_DB_BISON",
        "schema": "RAW",
        "target_table": "CITY_WEATHER",
    }


@task
def extract(cfg: dict) -> str:

    logical = get_logical_date()  # YYYY-MM-DD
    date_obj = datetime.strptime(logical, "%Y-%m-%d") - timedelta(days=1)
    date_to_fetch = date_obj.strftime("%Y-%m-%d")
    next_day = (date_obj + timedelta(days=1)).strftime("%Y-%m-%d")

    file_path = f"/tmp/{cfg['city']}_{date_to_fetch}.csv"
    save_weather_csv(
        cfg["city"],
        cfg["latitude"],
        cfg["longitude"],
        date_to_fetch,
        next_day,
        file_path
    )

    print(f"Saved CSV: {file_path}")
    return file_path


@task
def load(file_path: str, cfg: dict) -> None:

    logical = get_logical_date()
    date_obj = datetime.strptime(logical, "%Y-%m-%d") - timedelta(days=1)
    date_to_fetch = date_obj.strftime("%Y-%m-%d")
    next_day = (date_obj + timedelta(days=1)).strftime("%Y-%m-%d")

    db = cfg["database"]
    schema = cfg["schema"]
    table = cfg["target_table"]

    print(f"========= Incremental Updating {date_to_fetch}'s data ===========")

    cur = return_snowflake_cursor(SNOWFLAKE_CONN_ID)

    try:
        cur.execute("BEGIN;")

        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {db}.{schema};")

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {db}.{schema}.{table} (
                date DATE,
                temp_max FLOAT,
                temp_min FLOAT,
                precipitation FLOAT,
                weather_code INT,
                city VARCHAR
            );
        """)

        cur.execute(f"""
            DELETE FROM {db}.{schema}.{table}
            WHERE date >= '{date_to_fetch}' AND date < '{next_day}';
        """)

        populate_table_via_stage(cur, db, schema, table, file_path)

        cur.execute("COMMIT;")
        print(f"Committed incremental load for {date_to_fetch}")

    except Exception as e:
        cur.execute("ROLLBACK;")
        print("Rollback due to error:", e)
        raise

    finally:
        try:
            cur.close()
        except Exception:
            pass
# ---------------- DAG ----------------

with DAG(
    dag_id="weather_ETL_incremental",
    start_date=datetime(2026, 2, 28),
    catchup=False,
    tags=["ETL", "HW6"],
    schedule="30 3 * * *",
    max_active_runs=1,
) as dag:
    cfg = get_config()
    fp = extract(cfg)
    load(fp, cfg)