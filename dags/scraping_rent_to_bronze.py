
import logging
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from custom_modules.rent_extractor.apartment_extractor import GetApartmentsInfo
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from datetime import date 
from google.cloud import storage

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.DEBUG)
logger = logging.getLogger("airflow.task")
FILE_NAME=f"bronze/scraped_rent_sp_{date.today()}.parquet"
BUCKET_NAME="rent-extraction-us"

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
    parquet_table=pa.Table.from_pandas(extraction_pd)
    gcs_client=storage.Client()
    bucket=gcs_client.get_bucket(BUCKET_NAME)
    blob=bucket.blob(FILE_NAME)
    with blob.open('wb') as file:
        pq.write_table(parquet_table,file)



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

    extract_rent_to_parquet_bronze 

