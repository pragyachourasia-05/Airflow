from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime

DBT_PROJECT_DIR = Variable.get(
    "lab2_dbt_project_dir",
    default_var="/opt/airflow/dbt/olist_dbt"
)

with DAG(
    dag_id="lab2_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["lab2", "snowflake", "dbt"],
) as dag:

    etl_task = SnowflakeOperator(
        task_id="etl_task",
        snowflake_conn_id="snowflake_conn",
        sql="""
        BEGIN;

        BEGIN
            CREATE SCHEMA IF NOT EXISTS USER_DB_DRAGON.RAW;

            DELETE FROM USER_DB_DRAGON.RAW.RAW_ORDERS
            WHERE CAST(ORDER_PURCHASE_TIMESTAMP AS DATE) = CURRENT_DATE();

            INSERT INTO USER_DB_DRAGON.RAW.RAW_ORDERS
            SELECT *
            FROM USER_DB_DRAGON.RAW.RAW_ORDERS
            WHERE CAST(ORDER_PURCHASE_TIMESTAMP AS DATE) = CURRENT_DATE();

            COMMIT;

        EXCEPTION
            WHEN OTHER THEN
                ROLLBACK;
                RAISE;
        END;
        """
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt run --profiles-dir /opt/airflow/.dbt > /tmp/dbt_run.log 2>&1 || true
        grep -Eq "Completed successfully" /tmp/dbt_run.log
        """
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt test --profiles-dir /opt/airflow/.dbt > /tmp/dbt_test.log 2>&1 || true
        grep -Eq "Completed successfully|Nothing to do" /tmp/dbt_test.log
        """
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && \
        dbt snapshot --profiles-dir /opt/airflow/.dbt > /tmp/dbt_snapshot.log 2>&1 || true
        grep -Eq "Completed successfully" /tmp/dbt_snapshot.log
        """
    )

    etl_task >> dbt_run >> dbt_test >> dbt_snapshot
