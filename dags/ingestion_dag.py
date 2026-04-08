"""
DAG de ingestão — roda diariamente às 8h UTC.
Chama ingest_api.py e ingest_scraper.py em sequência.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "matheus",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="ingestion_dag",
    description="Ingere vagas da Adzuna API e RemoteOK scraper",
    schedule_interval="0 8 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "project3"],
) as dag:

    def run_api_ingestion():
        import sys
        sys.path.insert(0, "/opt/airflow/src")
        from ingestion.ingest_api import run
        result = run(raw_dir="/opt/airflow/data/raw")
        print(f"API ingestion result: {result}")

    def run_scraper_ingestion():
        import sys
        sys.path.insert(0, "/opt/airflow/src")
        from ingestion.ingest_scraper import run
        result = run(raw_dir="/opt/airflow/data/raw")
        print(f"Scraper ingestion result: {result}")

    task_api = PythonOperator(
        task_id="ingest_adzuna_api",
        python_callable=run_api_ingestion,
    )

    task_scraper = PythonOperator(
        task_id="ingest_remoteok_scraper",
        python_callable=run_scraper_ingestion,
    )

    task_api >> task_scraper