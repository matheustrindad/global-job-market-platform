"""
DAG de processamento — roda diariamente às 9h UTC.
Bronze (raw JSON) → Silver (trusted Parquet) + Quarantine
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "matheus",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="processing_dag",
    description="PySpark: raw JSON → trusted Parquet + quarantine",
    schedule_interval="0 9 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["processing", "pyspark", "project3"],
) as dag:

    def run_processing():
        import sys
        import os
        sys.path.insert(0, "/opt/airflow/src")

        # Garante que JAVA_HOME está setado para o PySpark
        os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
        os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python"

        from processing.process_jobs import run
        result = run(
            raw_dir="/opt/airflow/data/raw",
            trusted_dir="/opt/airflow/data/trusted",
            quarantine_dir="/opt/airflow/data/quarantine",
        )
        print(f"Processing result: {result}")
        if result["valid"] == 0:
            raise ValueError("Zero valid records — check raw data")

    PythonOperator(
        task_id="pyspark_raw_to_trusted",
        python_callable=run_processing,
        execution_timeout=timedelta(minutes=30),
    )