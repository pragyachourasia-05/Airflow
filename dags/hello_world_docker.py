from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

with DAG(
    dag_id="hello_world_docker",
    start_date=datetime(2026, 3, 19),
    schedule_interval="@once",
    catchup=False
) as dag:
    hello_task = DockerOperator(
        task_id="print_hello",
        image="etl_hello",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False
    )

    goodbye_task = DockerOperator(
        task_id="print_goodbye",
        image="etl_goodbye",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False
    )

    hello_task >> goodbye_task