from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# MON REPERTOIRE REEL
PROJECT_ROOT = "/home/mountah_lodia/ecommerce_project/ecommerce_project"
DATA_DIR = f"{PROJECT_ROOT}/data"
SCRIPTS_DIR = f"{PROJECT_ROOT}/airflow/scripts"

def on_failure(context):
    task = context.get("task_instance")
    print(f"❌ Échec : {task.task_id}")

def on_success(context):
    task = context.get("task_instance")
    print(f"✅ Succès : {task.task_id}")

def summary(**context):
    dag_run = context["dag_run"]
    print("📌 Résumé :", dag_run.run_id)
    print("État :", dag_run.get_state())

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": on_failure,
    "on_success_callback": on_success,
}

with DAG(
    dag_id="ecommerce_data_pipeline",
    description="Pipeline ETL : Bronze → Silver → Gold",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
) as dag:

    ingest_raw = BashOperator(
        task_id="ingest_raw_data",
        bash_command=f"""
            python3 {SCRIPTS_DIR}/ingest_raw.py \
            {DATA_DIR}/raw/data.csv \
            {DATA_DIR}
        """
    )

    transform_silver = BashOperator(
        task_id="transform_silver_layer",
        bash_command=f"""
            python3 {SCRIPTS_DIR}/transform_silver.py \
            {DATA_DIR} \
            {DATA_DIR}
        """
    )

    build_gold = BashOperator(
        task_id="build_features_gold",
        bash_command=f"""
            python3 {SCRIPTS_DIR}/build_features_gold.py \
            {DATA_DIR} \
            {DATA_DIR}
        """
    )

    monitoring = PythonOperator(
        task_id="monitoring_report",
        python_callable=summary,
        provide_context=True,
    )

    ingest_raw >> transform_silver >> build_gold >> monitoring
