import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace,trim
import logging



spark = SparkSession.builder.enableHiveSupport().getOrCreate()
gcs_bucket = "rent-extraction"
parquet_file_path = "bronze/scraped_rent_sp_*"
print("Writing Silver Table")
df_transformed = spark.read.parquet(f'gs://{gcs_bucket}/{parquet_file_path}')\
    .withColumn("price",trim((regexp_replace(col("price"),"[^0-9]",""))).cast("Integer"))\
    .withColumn("total_price",trim((regexp_replace(col("total_price"),"[^0-9]",""))).cast("Integer"))\
    .withColumn("address",col("address").cast("String"))\
    .withColumn("floor_size",trim((regexp_replace(col("floor_size"),"[^0-9]",""))).cast("Integer"))\
    .withColumn("number_of_rooms",trim((col("number_of_rooms"))).cast("Integer"))\
    .withColumn("number_of_bathrooms",trim((col("number_of_bathrooms"))).cast("Integer"))\


df_silver = df_transformed.dropDuplicates().na.drop()
df_silver.write.mode("overwrite").parquet(f"gs://{gcs_bucket}/silver/silver_rent_extraction.parquet")
    
print(f"Finished! Rows Created: {df_silver.count()}")

