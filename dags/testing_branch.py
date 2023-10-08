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
from airflow.utils.state import State


CLUSTER_NAME = 'bronze-to-silver-cluster'
REGION='us-east1'
PROJECT_ID='rent-extract-project'
PYSPARK_URI_SILVER='gs://spark-scripts-rent-project/spark_rent_extraction_bronze_to_silver.py'
PYSPARK_URI_GOLD='gs://spark-scripts-rent-project/spark_rent_extraction_silver_to_gold.py'
INIT_BUCKET="gold-init-script"
INIT_FILE="pip-install.sh"
DATASET_NAME="rent_extraction_dataset"
DATA_SAMPLE_GCS_BUCKET_NAME="rent-extraction-us"
DATA_SAMPLE_GCS_OBJECT_NAME='gold/gold_rent_extraction.parquet/*.parquet'



def choose_task_to_create_dataset(upstream_task_id, dag_run):
    upstream_task_state = dag_run.get_task_instance(upstream_task_id).state
    if upstream_task_state == State.FAILED:
        return "create_dataset"
    elif upstream_task_state == State.SUCCESS:
        return "create_sao_paulo_rent_analisys"

def choose_task_to_update_table(upstream_task_id, dag_run):
    upstream_task_state = dag_run.get_task_instance(upstream_task_id).state
    if upstream_task_state == State.FAILED:
        return "update_table_sao_paulo_rent_analisys"



with DAG(
    dag_id='teste',
    start_date=datetime(2021, 1, 1),
    schedule_interval="0 0 * * *",
    catchup=False
) as dag:

   

    get_dataset = BigQueryGetDatasetOperator(task_id="get-dataset", dataset_id=DATASET_NAME,retries=0)

    

    branch_task_creation = BranchPythonOperator(
        task_id='branch_task_creation',
        python_callable=choose_task_to_create_dataset,
        op_args=[get_dataset.task_id],
        trigger_rule="all_done",
        provide_context=True,
        dag=dag,
    )

    create_dataset = BigQueryCreateEmptyDatasetOperator(task_id="create_dataset", dataset_id=DATASET_NAME,retries=0)
    
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
        {"name": "latitude", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "longitude", "type": "FLOAT64", "mode": "REQUIRED"},        
    ],
    retries=0,
    location="us-west1"
    
)


    branch_task_update = BranchPythonOperator(
            task_id='branch_task_update',
            python_callable=choose_task_to_update_table,
            op_args=[create_sao_paulo_rent_analisys.task_id],
            trigger_rule="all_done",
            provide_context=True,
            dag=dag,
        )
    update_table_sao_paulo_rent_analisys = BigQueryUpdateTableOperator(
        task_id="update_table_sao_paulo_rent_analisys",
        dataset_id=DATASET_NAME,
        table_id="sao_paulo_rent_analisys",
        table_resource={
        "friendlyName": "Updated Table",
        "description": "Updated Table",
         },
        retries=0
    )

    get_dataset>>branch_task_creation
    branch_task_creation >> create_dataset
    branch_task_creation >> create_sao_paulo_rent_analisys
    create_dataset >> create_sao_paulo_rent_analisys
    create_sao_paulo_rent_analisys >> branch_task_update
    branch_task_update >> update_table_sao_paulo_rent_analisys
    