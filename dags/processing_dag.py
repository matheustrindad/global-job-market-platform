"""
DAG de processamento — roda após a ingestão (sensor no ingestion_dag).
Bronze (raw JSON) → Silver (trusted Parquet) + Quarantine
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    "owner": "matheus",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="processing_dag",
    description="PySpark: raw JSON → trusted Parquet + quarantine",
    schedule_interval="0 9 * * *",   # 1h após a ingestão (8h)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["processing", "pyspark", "project3"],
) as dag:

    # Espera o ingestion_dag terminar antes de processar
    wait_for_ingestion = ExternalTaskSensor(
        task_id="wait_for_ingestion",
        external_dag_id="ingestion_dag",
        external_task_id=None,   # None = espera o DAG inteiro
        allowed_states=["success"],
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    def run_processing():
        import sys
        sys.path.insert(0, "/opt/airflow/src")
        from processing.process_jobs import run
        result = run(
            raw_dir="/opt/airflow/data/raw",
            trusted_dir="/opt/airflow/data/trusted",
            quarantine_dir="/opt/airflow/data/quarantine",
        )
        print(f"Processing result: {result}")
        if result["valid"] == 0:
            raise ValueError("Zero valid records after processing — check raw data")

    task_process = PythonOperator(
        task_id="pyspark_raw_to_trusted",
        python_callable=run_processing,
    )

    wait_for_ingestion >> task_process
