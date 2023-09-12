from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io

def create_and_upload_parquet_to_s3():
    # Create a Pandas DataFrame (replace this with your actual data)
    data = {'column1': [1, 2, 3], 'column2': ['A', 'B', 'C']}
    df = pd.DataFrame(data)

    # Convert the DataFrame to a PyArrow Table
    table = pa.Table.from_pandas(df)

    # Create a BytesIO buffer to hold the Parquet data
    buffer = io.BytesIO()

    # Write the PyArrow Table as a Parquet file to the buffer
    pq.write_table(table, buffer)

    # Specify your S3 bucket and object (destination path) information
    s3_bucket_name="rent-extraction"
    s3_object_key="source/test.parquet"

    # Initialize the S3Hook
    s3_hook = S3Hook(aws_conn_id="aws_default")  # "aws_default" should match your MWAA connection ID

    # Upload the Parquet data to S3
    with buffer as buffer_file:
        s3_hook.load_bytes(buffer_file.read(), s3_object_key, bucket_name=s3_bucket_name)

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

# Task to create and upload the Parquet file to S3
upload_task = PythonOperator(
    task_id="upload_parquet_to_s3_task",
    python_callable=create_and_upload_parquet_to_s3,
    dag=dag,
)
