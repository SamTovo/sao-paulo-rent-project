from airflow import DAG
from datetime import timedelta,datetime
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import  DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator



CLUSTER_NAME = 'bronze-to-silver-cluster'
REGION='us-west2'
PROJECT_ID='rent-extract-project'
PYSPARK_URI='gs://spark-scripts-rent-project/spark_rent_extraction_bronze_to_silvel.py'

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    }
}


PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}

with DAG(
    dag_id='bronze_to_silver_rent_dataproc',
    start_date=datetime(2021, 1, 1),
    schedule="0 0 * * *",
    catchup=False
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME
    )

    execute_spark_bronze_to_silver_rent = DataprocSubmitJobOperator(
        task_id="execute_spark_bronze_to_silver_rent", 
        job=PYSPARK_JOB, 
        region=REGION, 
        project_id=PROJECT_ID,

    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", 
        project_id=PROJECT_ID, 
        cluster_name=CLUSTER_NAME, 
        region=REGION
    )

    create_cluster >> execute_spark_bronze_to_silver_rent >> delete_cluster