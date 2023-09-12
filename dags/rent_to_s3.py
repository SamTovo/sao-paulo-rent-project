
import os
import logging
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook

from custom_modules.rent_extractor.apartment_extractor import GetApartmentsInfo
import io
import pandas as pd
from datetime import date 
import pyarrow as pa
import pyarrow.parquet as pq
import fsspec
import s3fs
logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.DEBUG)

logger = logging.getLogger("airflow.task")


def day_of_week():
    dt=datetime.now()
    day_of_week=dt.weekday()
    return day_of_week

def define_page_range_for_request():
    first_page=day_of_week()*10
    last_page=first_page+10
    return first_page, last_page


def extract_rent_etl():
    first_page, last_page = define_page_range_for_request()
    extraction=GetApartmentsInfo(first_page,last_page)
    extraction_pd=extraction.generate_pandas_apartment_info()
    table = pa.Table.from_pandas(extraction_pd)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)
    s3_bucket_name="rent-extraction"
    s3_object_key=f"source/rent_extraction_{date.today()}.parquet"
    s3_hook = S3Hook(aws_conn_id="aws_default")
    with buffer as buffer_file:
        s3_hook.load_bytes(buffer_file.read(), s3_object_key, bucket_name=s3_bucket_name)


    



with DAG(
    dag_id="rent_to_s3",
    start_date=datetime(2021, 1, 1),
    schedule="0 0 * * *",
    catchup=False
) as dag:
   
    extract_rent_to_parquet = PythonOperator(
        task_id = 'extract_rent_to_parquet',
        python_callable = extract_rent_etl,
        provide_context = True, 
    )
    extract_rent_to_parquet

