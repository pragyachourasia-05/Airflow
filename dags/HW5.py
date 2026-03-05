from __future__ import annotations

from datetime import datetime, timedelta, date
import requests

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

SNOWFLAKE_CONN_ID = "snowflake_conn"

def get_snowflake_cursor():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    return conn.cursor()

@task
def read_lat_lon() -> dict:

    lat = float(Variable.get("latitude"))
    lon = float(Variable.get("longitude"))
    print(f"Using latitude={lat}, longitude={lon}")
    return {"latitude": lat, "longitude": lon}


@task
def compute_date_range() -> dict:

    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=59)
    return {
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
    }

@task
def extract_weather(latlon: dict, dr: dict) -> dict:

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latlon["latitude"],
        "longitude": latlon["longitude"],
        "start_date": dr["start_date"],
        "end_date": dr["end_date"],
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


@task
def transform_to_records(raw_json: dict, latlon: dict) -> list[dict]:

    daily = raw_json.get("daily", {})
    dates = daily.get("time", [])
    tmax = daily.get("temperature_2m_max", [])
    tmin = daily.get("temperature_2m_min", [])
    precip = daily.get("precipitation_sum", [])
    wcode = daily.get("weathercode", [])

    n = len(dates)
    records = []

    for i in range(n):
        records.append(
            {
                "latitude": latlon["latitude"],
                "longitude": latlon["longitude"],
                "date": dates[i],  # already YYYY-MM-DD
                "temp_max": tmax[i] if i < len(tmax) else None,
                "temp_min": tmin[i] if i < len(tmin) else None,
                "precipitation": precip[i] if i < len(precip) else None,
                "weather_code": wcode[i] if i < len(wcode) else None,
            }
        )

    print(f"Transformed {len(records)} records")
    return records
@task
def load_full_refresh(records: list[dict]) -> str:
    schema = Variable.get("weather_schema", default_var="RAW")
    table = Variable.get("weather_table", default_var="WEATHER_DAILY")
    fq_table = f"{schema}.{table}"

    cur = get_snowflake_cursor()
    try:
        cur.execute("BEGIN;")

        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {fq_table} (
                latitude FLOAT NOT NULL,
                longitude FLOAT NOT NULL,
                date DATE NOT NULL,
                temp_max FLOAT,
                temp_min FLOAT,
                precipitation FLOAT,
                weather_code INTEGER,
                PRIMARY KEY (latitude, longitude, date)
            );
            """
        )

        cur.execute(f"DELETE FROM {fq_table};")
        for r in records:
            cur.execute(
                f"""
                INSERT INTO {fq_table}
                    (latitude, longitude, date, temp_max, temp_min, precipitation, weather_code)
                VALUES
                    ({r['latitude']}, {r['longitude']}, '{r['date']}',
                     {r['temp_max'] if r['temp_max'] is not None else 'NULL'},
                     {r['temp_min'] if r['temp_min'] is not None else 'NULL'},
                     {r['precipitation'] if r['precipitation'] is not None else 'NULL'},
                     {r['weather_code'] if r['weather_code'] is not None else 'NULL'});
                """
            )

        cur.execute("COMMIT;")
        msg = f"Loaded {len(records)} records into {fq_table}"
        print(msg)
        return msg

    except Exception:
        try:
            cur.execute("ROLLBACK;")
        except Exception:
            pass
        raise

    finally:
        try:
            cur.close()
        except Exception:
            pass


with DAG(
    dag_id="HW5_Weather_ETL",
    start_date=datetime(2026, 2, 23),
    schedule="30 2 * * *",
    catchup=False,
    tags=["HW5", "Weather", "ETL"],
) as dag:
    latlon = read_lat_lon()
    dr = compute_date_range()
    raw = extract_weather(latlon, dr)
    recs = transform_to_records(raw, latlon)
    load_full_refresh(recs)