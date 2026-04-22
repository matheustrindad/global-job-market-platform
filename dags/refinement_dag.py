"""
DAG de refinamento — roda após o processamento (10h UTC).
trusted/ → refined/ (3 agregações analíticas)
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
    dag_id="refinement_dag",
    description="PySpark: trusted Parquet → refined aggregations",
    schedule_interval="0 10 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["processing", "refined", "project3"],
) as dag:

    def run_refinement():
        import sys
        import os
        sys.path.insert(0, "/opt/airflow/src")
        os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
        os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python"

        from processing.refine_jobs import run
        result = run(
            trusted_dir="/opt/airflow/data/trusted",
            refined_dir="/opt/airflow/data/refined",
        )
        print(f"Refinement result: {result}")

    PythonOperator(
        task_id="pyspark_trusted_to_refined",
        python_callable=run_refinement,
        execution_timeout=timedelta(minutes=30),
    )