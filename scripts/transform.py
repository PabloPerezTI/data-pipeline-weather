from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, udf
from pyspark.sql.types import StringType, IntegerType, FloatType, StructType, StructField

spark = SparkSession.builder.appName("Weather Data Transformation").getOrCreate()

def transform_data(filename):
    df = spark.read.parquet(filename, header=True)
    df.createOrReplaceTempView('weather_data')
    df = spark.sql("SELECT name, datetime, tempmax, tempmin, temp as tempavg, precip, precipprob, precipcover, preciptype,snow, windspeed,cloudcover, sunrise, sunset FROM weather_data")
    



if __name__ == "__main__":
    transform_data('data/Spain/Spain_2023_2023.parquet')