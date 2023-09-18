import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace,trim
import logging

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.DEBUG)

logger = logging.getLogger()
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
gcs_bucket = "gs://rent-extraction"
parquet_file_path = "bronze/scraped_rent_sp_*"
logger.info("Writing Silver Table")
df = spark.read.parquet(f"{gcs_bucket}/{parquet_file_path}")\
    .withColumn("price",regexp_replace("price","R$",""),trim("price"),col("price").cast("Integer"))\
    .withColumn("total_price",regexp_replace("total_price","R$",""),trim("total_price"),col("price").cast("Integer"))\
    .withColumn("address",col("address").cast("String"))\
    .withColumn("address",col("address").cast("String"))\
    .withColumn("floor_size",regexp_replace("floor_size","M2",""),trim("floor_size"),col("floor_size").cast("Integer"))\
    .withColumn("number_of_rooms",trim("number_of_rooms"),col("number_of_rooms").cast("Integer"))\
    .withColumn("number_of_bathrooms",trim("number_of_bathrooms"),col("number_of_bathrooms").cast("Integer"))
    
df.write.mode("overwrite").parquet(f"{gcs_bucket}/silver/silver_rent_extraction")
    
logger.info(f"Finished! Rows Created: {df.count()}")

