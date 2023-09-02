
import os
import logging
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from custom_modules.rent_extractor.apartment_extractor import GetApartmentsInfo
import io


import awswrangler as wr
import pandas as pd
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook





def day_of_week():
    dt=datetime.now()
    day_of_week=dt.weekday()
    return day_of_week

def define_page_range_for_request():
    first_page=day_of_week()*100
    last_page=first_page+100
    return first_page, last_page


def extract_rent(**kwargs):
    # ti = kwargs['ti']
    first_page, last_page = define_page_range_for_request()
    extraction=GetApartmentsInfo(first_page,last_page)
    extraction_pd=extraction.generate_pandas_apartment_info()
    print(extraction_pd)
    return extraction_pd
    # ti.xcom_push(key = 'rent_extraction' , value = extraction_pd.to_parquet(s3_url))


def upload_to_s3():
    df: pd.DataFrame =  extract_rent()
    key = 'bronze/rent_extraction.parquet'
    hook = AwsBaseHook(aws_conn_id="AWS_CONN")

    # upload to S3 bucket
    wr.s3.to_parquet(df=df, path=f"s3://rent-extraction/{key}", boto3_session=hook.get_session())

with DAG(
    dag_id="rent_to_s3",
    start_date=datetime(2021, 1, 1),
    schedule="0 0 * * *",
    catchup=False
) as dag:
   
    extract_rent_to_parquet = PythonOperator(
        task_id = 'extract_rent_to_parquet',
        python_callable = extract_rent,
        provide_context = True, 
    )
    upload_prices_to_s3 = PythonOperator(
        task_id = 'upload_prices_to_s3',
        python_callable = upload_to_s3,
        provide_context = True, 
    )
    extract_rent_to_parquet >> upload_prices_to_s3

