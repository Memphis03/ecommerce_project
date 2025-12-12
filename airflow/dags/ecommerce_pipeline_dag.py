from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

PROJECT_ROOT = "/home/mountah_lodia/ecommerce_project/ecommerce_project"
DATA_DIR = f"{PROJECT_ROOT}/data"
SCRIPTS_DIR = f"{PROJECT_ROOT}/airflow/scripts"

def on_failure(context):
    print(f"❌ Échec : {context['task_instance'].task_id}")

def on_success(context):
    print(f"✅ Succès : {context['task_instance'].task_id}")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": on_failure,
    "on_success_callback": on_success,
}

with DAG(
    dag_id="ecommerce_data_pipeline",
    description="Pipeline ETL Bronze → Silver → Gold",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
) as dag:

    ingest = BashOperator(
        task_id="ingest_raw",
        bash_command=f"python3 {SCRIPTS_DIR}/ingest_raw.py {DATA_DIR}/raw/data.csv {DATA_DIR}"
    )

    silver = BashOperator(
        task_id="silver_transform",
        bash_command=f"python3 {SCRIPTS_DIR}/transform_silver.py {DATA_DIR} {DATA_DIR}"
    )

    gold = BashOperator(
        task_id="gold_features",
        bash_command=f"python3 {SCRIPTS_DIR}/build_features_gold.py {DATA_DIR} {DATA_DIR}"
    )

    ingest >> silver >> gold
