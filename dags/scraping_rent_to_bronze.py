
import logging
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from custom_modules.rent_extractor.apartment_extractor import GetApartmentsInfo
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from datetime import date 
import os
import gcsfs
from io import BytesIO
from google.cloud import storage

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.DEBUG)
logger = logging.getLogger("airflow.task")
FILE_NAME=f"/bronze/scraped_rent_sp_{date.today()}.parquet"
BUCKET_NAME="rent-extraction"

def day_of_week():
    dt=datetime.now()
    day_of_week=dt.weekday()
    return day_of_week

def define_page_range_for_request():
    first_page=day_of_week()*10
    last_page=first_page+10
    return first_page, last_page

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """


    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_file(local_file)


def extract_rent_etl():
    buffer=BytesIO()
    first_page, last_page = define_page_range_for_request()
    extraction=GetApartmentsInfo(first_page,last_page)
    extraction_pd=extraction.generate_pandas_apartment_info()
    parquet_buffer = BytesIO()
    table = pa.Table.from_pandas(extraction_pd)
    pq.write_table(table, parquet_buffer)
    parquet_buffer.seek(0)
    parquet_bytes = parquet_buffer.read()
    assert isinstance(parquet_bytes, bytes)
    upload_to_gcs(BUCKET_NAME,FILE_NAME,parquet_bytes)



default_args = {
    'depends_on_past': False    
}
with DAG(
    dag_id="scraping_rent_to_bronze",
    start_date=datetime(2021, 1, 1),
    schedule_interval="0 0 * * *",
    default_args=default_args,
    catchup=False
    ) as dag:
   
    extract_rent_to_parquet_bronze = PythonOperator(
        task_id = 'extract_rent_to_parquet_in_gcs',
        python_callable = extract_rent_etl,
        provide_context = True, 
    )

    # upload_file = LocalFilesystemToGCSOperator(
    #     task_id="upload_file",
    #     src=extract_rent_to_parquet_bronze.output,
    #     dst=FILE_NAME,
    #     bucket=BUCKET_NAME,
    #     retries=0
    # )
    extract_rent_to_parquet_bronze 

