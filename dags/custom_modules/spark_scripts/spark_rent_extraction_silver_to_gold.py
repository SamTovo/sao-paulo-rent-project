from pyspark.sql.functions import col, lit,split, mean, stddev
import googlemaps
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

def z_score_outlier_treatment(df, columns, threshold=3.0):
    """
    Detects and removes outliers from a PySpark DataFrame using the z-score method.

    :param df: PySpark DataFrame
    :param columns: list of column names for which outliers should be removed
    :param threshold: z-score threshold to determine outliers (default is 3.0)
    :return: PySpark DataFrame with outliers removed
    """
    for column in columns:
        # Calculate mean and standard deviation
        stats = df.select(mean(col(column)).alias('mean'), stddev(col(column)).alias('stddev')).collect()[0]

        # Calculate z-scores and filter outliers
        df = df.withColumn(f'{column}_z_score', (col(column) - stats['mean']) / stats['stddev']) \
               .filter(f'abs({column}_z_score) <= {threshold}') \
               .drop(f'{column}_z_score')

    return df


def geocode(address,code):
    allowed_values = ('lat', 'lng') 
    assert code in allowed_values, f"Input must be one of {allowed_values}, but received: {code}"
    gmaps = googlemaps.Client(key='AIzaSyBuMQ1BQ1lJ883amFdrooTmqyGqjQUOllE')
    geocode_result = gmaps.geocode(address)
    for result in geocode_result:
        return result['geometry']['location'][code]
    
geocode_udf = udf(lambda address, code: geocode(address, code), DoubleType())

gcs_bucket = "rent-extraction-us"
parquet_file_path = "silver/silver_rent_extraction.parquet"

df_transformed = spark.read.parquet(f'gs://{gcs_bucket}/{parquet_file_path}')\
    .withColumn("latitude",geocode_udf(col("address"), lit("lat")))\
    .withColumn("longitude",geocode_udf(col("address"), lit("lng")))\
    .withColumn("neighborhood",split(col("address"),",").getItem(1))

df_gold = df_transformed.dropDuplicates().na.drop()
df_gold_treated=z_score_outlier_treatment(df_gold,['total_price','latitude','longitude','floor_size'],threshold=3.0)
df_gold_treated.write.mode("overwrite").parquet(f"gs://{gcs_bucket}/gold/gold_rent_extraction.parquet")