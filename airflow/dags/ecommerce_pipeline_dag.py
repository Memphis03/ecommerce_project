from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="ecommerce_data_pipeline",
    description="Pipeline ETL complet: Bronze → Silver → Gold",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
) as dag:

    ingest_raw = BashOperator(
        task_id="ingest_raw_data",
        bash_command=(
            "python3 /usr/local/airflow/scripts/ingest_raw.py "
            "/usr/local/ecommerce_project/data/raw/data.csv "
            "/usr/local/ecommerce_project/data"
        )
    )

    transform_silver = BashOperator(
        task_id="transform_silver_layer",
        bash_command=(
            "python3 /usr/local/airflow/scripts/transform_silver.py "
            "/usr/local/ecommerce_project/data "
            "/usr/local/ecommerce_project/data"
        )
    )

    build_gold = BashOperator(
        task_id="build_features_gold",
        bash_command=(
            "python3 /usr/local/airflow/scripts/build_features_gold.py "
            "/usr/local/ecommerce_project/data "
            "/usr/local/ecommerce_project/data"
        )
    )

    ingest_raw >> transform_silver >> build_gold
