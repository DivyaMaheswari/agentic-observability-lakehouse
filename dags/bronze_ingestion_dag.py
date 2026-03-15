from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "Divya",
    "start_date": datetime(2025, 1, 1),
    "retries": 1
}

with DAG(
    dag_id="bronze_ingestion_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    run_spark_bronze = BashOperator(
        task_id="run_bronze_ingestion",
        bash_command="""
        docker exec spark /opt/spark/bin/spark-submit \
        --master spark://spark:7077 \
        --packages io.delta:delta-spark_2.12:3.1.0 \
        --conf spark.jars.ivy=/tmp/.ivy \
        /opt/spark_jobs/bronze/bronze_ingestion.py
        """
    )

    run_spark_bronze