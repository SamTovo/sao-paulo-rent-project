from airflow import DAG
from datetime import timedelta,datetime
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import  (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryGetDatasetOperator,
    BigQueryCreateExternalTableOperator,
    BigQueryUpdateTableOperator
)


CLUSTER_NAME = 'bronze-to-silver-cluster'
REGION='us-west2'
PROJECT_ID='rent-extract-project'
PYSPARK_URI_SILVER='gs://spark-scripts-rent-project/spark_rent_extraction_bronze_to_silver.py'
PYSPARK_URI_GOLD='gs://spark-scripts-rent-project/spark_rent_extraction_silver_to_gold.py'
INIT_BUCKET="gold-init-script"
INIT_FILE="pip-install.sh"
DATASET_NAME="rent_extraction_dataset   "
DATA_SAMPLE_GCS_BUCKET_NAME="rent-extraction"
DATA_SAMPLE_GCS_OBJECT_NAME='gold/gold_rent_extraction.parquet'

CLUSTER_GENERATOR_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    zone=REGION,
    master_machine_type="n1-standard-2",
    master_disk_size=32,
    worker_machine_type="n1-standard-2",
    worker_disk_size=32,
    num_workers=2,
    init_actions_uris=[f"gs://{INIT_BUCKET}/{INIT_FILE}"],
    metadata={"PIP_PACKAGES": "googlemaps"}
).make()



PYSPARK_JOB_SILVER = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI_SILVER},
}

PYSPARK_JOB_GOLD = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI_GOLD},
}


def decide_branch_create(**kwargs):
    task_instance = kwargs['task_instance']
    
    # Check if the upstream task has failed
    if task_instance.xcom_pull(task_ids='get-dataset', key='return_value') == 'failed':
        return 'create_dataset'
    else:
        return 'create_sao_paulo_rent_analisys'

def decide_branch_update(**kwargs):
    task_instance = kwargs['task_instance']
    if task_instance.xcom_pull(task_ids='create_sao_paulo_rent_analisys', key='return_value') == 'failed':
        return 'update_table_sao_paulo_rent_analisys'


with DAG(
    dag_id='bronze_to_silver_gold_rent_dataproc',
    start_date=datetime(2021, 1, 1),
    schedule_interval="0 0 * * *",
    catchup=False
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_GENERATOR_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,

    )

    execute_spark_bronze_to_silver_rent = DataprocSubmitJobOperator(
        task_id="execute_spark_bronze_to_silver_rent", 
        job=PYSPARK_JOB_SILVER, 
        region=REGION, 
        project_id=PROJECT_ID,

    )


    execute_spark_silver_to_gold_rent = DataprocSubmitJobOperator(
        task_id="execute_spark_silver_to_gold_rent", 
        job=PYSPARK_JOB_GOLD, 
        region=REGION, 
        project_id=PROJECT_ID,

    )
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", 
        project_id=PROJECT_ID, 
        cluster_name=CLUSTER_NAME, 
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE
    )

    get_dataset = BigQueryGetDatasetOperator(task_id="get-dataset", dataset_id=DATASET_NAME)

    create_dataset = BigQueryCreateEmptyDatasetOperator(task_id="create_dataset", dataset_id=DATASET_NAME)

    branch_task_creation = BranchPythonOperator(
        task_id='branch_task_creation',
        python_callable=decide_branch_create,
        provide_context=True,
        dag=dag,
    )


    branch_task_update = BranchPythonOperator(
        task_id='branch_task_update',
        python_callable=decide_branch_update,
        provide_context=True,
        dag=dag,
    )
    create_sao_paulo_rent_analisys = BigQueryCreateExternalTableOperator(
    task_id="create_sao_paulo_rent_analisys",
    destination_project_dataset_table=f"{DATASET_NAME}.sao_paulo_rent_analisys",
    bucket=DATA_SAMPLE_GCS_BUCKET_NAME,
    source_objects=[DATA_SAMPLE_GCS_OBJECT_NAME],
    schema_fields=[
        {"name": "price", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "total_price", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "address", "type": "STRING", "mode": "REQUIRED"},
        {"name": "floor_size", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "number_of_rooms", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "number_of_bathrooms", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "latitude", "type": "DOUBLE", "mode": "REQUIRED"},
        {"name": "longitude", "type": "DOUBLE", "mode": "REQUIRED"},        
    ],
)

    update_table_sao_paulo_rent_analisys = BigQueryUpdateTableOperator(
        task_id="update_table_sao_paulo_rent_analisys",
        dataset_id=DATASET_NAME,
        table_id="sao_paulo_rent_analisys",
        table_resource={
        "friendlyName": "Updated Table",
        "description": "Updated Table",
    },
    )

    create_cluster >> execute_spark_bronze_to_silver_rent 
    execute_spark_bronze_to_silver_rent >> execute_spark_silver_to_gold_rent 
    execute_spark_silver_to_gold_rent >> delete_cluster
    get_dataset >> branch_task_creation
    branch_task_creation >> create_dataset
    branch_task_creation >> create_sao_paulo_rent_analisys
    create_dataset >> create_sao_paulo_rent_analisys
    branch_task_update >> update_table_sao_paulo_rent_analisys
    