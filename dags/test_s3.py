
import os
import boto3
from botocore.exceptions import ClientError
import logging
import random

from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
import json
from pendulum import datetime, duration
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd


def api_call(**kwargs):
    
    ti = kwargs['ti']
    url = 'https://api.publicapis.org/entries'
    response = requests.get(url).json()
    df = pd.DataFrame(response['entries'])
    ti.xcom_push(key = 'final_data' , value = df.to_csv(index=False))

with DAG(
    dag_id="test_s3",
    start_date=datetime(2021, 1, 1),
    schedule="0 0 * * *",
    catchup=False
) as dag:
   
    api_hit_task = PythonOperator(
        task_id = 'API-Call',
        python_callable = api_call,
        provide_context = True,
        
    )
    Upload_to_s3 = S3CreateObjectOperator(
        task_id="Upload-to-S3",
        aws_conn_id= 'AWS_CONN',
        s3_bucket='testairflowbucketsamuel',
        s3_key='samuel/Api_Data.csv',
        data="{{ ti.xcom_pull(key='final_data') }}",    
    )
    api_hit_task >> Upload_to_s3

