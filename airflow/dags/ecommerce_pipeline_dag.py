from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta

PROJECT_ROOT = "/home/mountah_lodia/ecommerce_project/ecommerce_project"
DATA_DIR = f"{PROJECT_ROOT}/data"
SCRIPTS_DIR = f"{PROJECT_ROOT}/airflow/scripts"

# Callbacks pour succès / échec
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

# Récupération de la date de filtrage depuis Airflow Variable ou valeur par défaut
START_DATE_FILTER = Variable.get("start_date_filter", default_var="2011-05-17")

with DAG(
    dag_id="ecommerce_data_pipeline",
    description="Pipeline ETL Bronze → Silver → Gold (incrémental par date)",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),  # Date de planification Airflow
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["ecommerce", "ETL", "incremental"]
) as dag:

    # -------------------------
    # Ingestion RAW → Bronze
    # -------------------------
    ingest = BashOperator(
        task_id="ingest_raw",
        bash_command=(
            f"python3 {SCRIPTS_DIR}/ingest_raw.py "
            f"{DATA_DIR}/raw/data.csv {DATA_DIR} {START_DATE_FILTER}"
        )
    )

    # -------------------------
    # Transformation Bronze → Silver
    # -------------------------
    silver = BashOperator(
        task_id="silver_transform",
        bash_command=(
            f"python3 {SCRIPTS_DIR}/transform_silver.py "
            f"{DATA_DIR} {DATA_DIR} {START_DATE_FILTER}"
        )
    )

    # -------------------------
    # Création des features Gold
    # -------------------------
    gold = BashOperator(
        task_id="gold_features",
        bash_command=(
            f"python3 {SCRIPTS_DIR}/build_features_gold.py "
            f"{DATA_DIR} {DATA_DIR} {START_DATE_FILTER}"
        )
    )

    # -------------------------
    # Ordonnancement
    # -------------------------
    ingest >> silver >> gold
