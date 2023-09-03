
import os
import logging
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from custom_modules.rent_extractor.apartment_extractor import GetApartmentsInfo
import io
import pandas as pd
from datetime import date 




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
    extraction_pd.to_parquet(f"s3://rent-extraction/bronze/rent_extraction_{date.today()}.parquet")

    



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

