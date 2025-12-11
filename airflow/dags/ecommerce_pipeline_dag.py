from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


# ------------------------------
# 1. CALLBACKS DE MONITORING
# ------------------------------
def on_failure(context):
    task = context.get("task_instance")
    print(f"❌ Échec dans la tâche : {task.task_id}")


def on_success(context):
    task = context.get("task_instance")
    print(f"✅ Succès de la tâche : {task.task_id}")


def summary(**context):
    dag_run = context["dag_run"]
    print("📌 Résumé du DAG run :", dag_run.run_id)
    print("État final :", dag_run.get_state())


# ------------------------------
# 2. ARGUMENTS PAR DÉFAUT
# ------------------------------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": on_failure,
    "on_success_callback": on_success,
}


# ------------------------------
# 3. DÉFINITION DU DAG
# ------------------------------
with DAG(
    dag_id="ecommerce_data_pipeline",
    description="Pipeline ETL : Bronze → Silver → Gold",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
) as dag:

    # ---- BRONZE : ingestion ----
    ingest_raw = BashOperator(
        task_id="ingest_raw_data",
        bash_command=(
            "python3 /usr/local/airflow/scripts/ingest_raw.py "
            "/usr/local/ecommerce_project/data/raw/data.csv "
            "/usr/local/ecommerce_project/data"
        )
    )

    # ---- SILVER : nettoyage / transformation ----
    transform_silver = BashOperator(
        task_id="transform_silver_layer",
        bash_command=(
            "python3 /usr/local/airflow/scripts/transform_silver.py "
            "/usr/local/ecommerce_project/data "
            "/usr/local/ecommerce_project/data"
        )
    )

    # ---- GOLD : feature engineering ----
    build_gold = BashOperator(
        task_id="build_features_gold",
        bash_command=(
            "python3 /usr/local/airflow/scripts/build_features_gold.py "
            "/usr/local/ecommerce_project/data "
            "/usr/local/ecommerce_project/data"
        )
    )

    # ---- MONITORING FINAL ----
    monitoring = PythonOperator(
        task_id="monitoring_report",
        python_callable=summary,
        provide_context=True,
    )

    # pipeline
    ingest_raw >> transform_silver >> build_gold >> monitoring
