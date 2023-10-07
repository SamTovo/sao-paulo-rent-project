from pyspark.sql.functions import col, lit
import googlemaps
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

def geocode(address,code):
    allowed_values = ('lat', 'lng') 
    assert code in allowed_values, f"Input must be one of {allowed_values}, but received: {code}"
    gmaps = googlemaps.Client(key='AIzaSyBuMQ1BQ1lJ883amFdrooTmqyGqjQUOllE')
    geocode_result = gmaps.geocode(address)
    for result in geocode_result:
        return result['geometry']['location'][code]
    
geocode_udf = udf(lambda address, code: geocode(address, code), DoubleType())

gcs_bucket = "rent-extraction"
parquet_file_path = "silver/silver_rent_extraction.parquet"

df_transformed = spark.read.parquet(f'gs://{gcs_bucket}/{parquet_file_path}')\
    .withColumn("latitude",geocode_udf(col("address"), lit("lat")))\
    .withColumn("longitude",geocode_udf(col("address"), lit("lng")))\

df_gold = df_transformed.dropDuplicates().na.drop()
df_gold.write.mode("overwrite").parquet(f"gs://{gcs_bucket}/gold/gold_rent_extraction.parquet")