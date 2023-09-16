from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession, functions


# Create an Airflow DAG
dag = DAG(
    "upload_parquet_to_s3_dag",
    default_args={
        "owner": "your-name",
        "start_date": datetime(2023, 1, 1),
        # Add any other default_args as needed
    },
    schedule_interval=None,  # You can set a schedule if needed
    catchup=False,
)

