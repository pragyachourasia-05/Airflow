from __future__ import annotations

import json
import requests
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


def get_conn_and_cur():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn, conn.cursor()


@task
def etl_openmeteo_to_snowflake():
    """
    Fetch 60 days daily temperature metrics for each city in WEATHER_LOCATIONS,
    load into WEATHER_STAGE, then CALL MERGE_WEATHER().
    """

    # Airflow Variables:
    # WEATHER_LOCATIONS must be JSON list of dicts with keys: name, latitude, longitude
    locations = json.loads(Variable.get("WEATHER_LOCATIONS"))
    past_days = int(Variable.get("WEATHER_PAST_DAYS", default_var="60"))

    conn, cur = get_conn_and_cur()

    insert_stage_sql = """
        INSERT INTO WEATHER_STAGE
        (location_name, latitude, longitude, date, temp_max, temp_min, temp_mean)
         VALUES (%s, %s, %s, %s, %s, %s, %s);
    """

    try:
        cur.execute("BEGIN")
        cur.execute("USE DATABASE USER_DB_BISON")
        cur.execute("USE SCHEMA SCHEMA_WEATHER_PREDICTION")

        # clean stage each run
        cur.execute("TRUNCATE TABLE WEATHER_STAGE")

        for loc in locations:
            name = loc["name"]
            lat = float(loc["latitude"])
            lon = float(loc["longitude"])

            params = {
                "latitude": lat,
                "longitude": lon,
                "daily": "temperature_2m_max,temperature_2m_min,temperature_2m_mean",
                "past_days": past_days,
                "timezone": "auto",
            }

            r = requests.get(OPEN_METEO_URL, params=params, timeout=60)
            r.raise_for_status()
            data = r.json()

            daily = data.get("daily", {})
            dates = daily.get("time", [])
            tmax = daily.get("temperature_2m_max", [])
            tmin = daily.get("temperature_2m_min", [])
            tmean = daily.get("temperature_2m_mean", [])

            rows = []
            for i in range(len(dates)):
                if tmax[i] is None or tmin[i] is None or tmean[i] is None:
                    continue
                rows.append((name, lat, lon, dates[i], tmax[i], tmin[i], tmean[i]))

            if rows:
                cur.executemany(insert_stage_sql, rows)

        cur.execute("CALL USER_DB_BISON.SCHEMA_WEATHER_PREDICTION.MERGE_WEATHER()")
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
    dag_id="weather_etl_openmeteo",
    start_date=datetime(2026, 3, 1),
    schedule="0 2 * * *",  # 2:00 AM daily
    catchup=False,
    tags=["weather", "ETL"],
) as dag:
    etl_openmeteo_to_snowflake()