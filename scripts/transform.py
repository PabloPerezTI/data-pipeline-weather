from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, udf, split, to_timestamp, hour, minute,second, mean, round
from pyspark.sql.types import StringType, IntegerType, FloatType, StructType, StructField

spark = SparkSession.builder.appName("Weather Data Transformation").getOrCreate()

dfSpain = spark.read.parquet('data/Spain/Spain_2022.parquet')
dfGermany = spark.read.parquet('data/Germany/Germany_2022.parquet')

def selColumns(df):
    name_expr = when(col("name").contains("Schweiz"), lit("Switzerland")) \
                .when(col("name").contains("Deutschland"), lit("Germany")) \
                .when(col("name").contains("Nederland"), lit("Netherlands")) \
                .when(col("name").contains("Espa"), lit("Spain")) \
                .otherwise(col("name"))

    sunrise_minute_rounded = when(
        (second(to_timestamp("sunrise")) > 30) & (minute(to_timestamp("sunrise")) != 59), minute(to_timestamp("sunrise")) + 1) \
        .otherwise(minute(to_timestamp("sunrise")))

    sunset_minute_rounded = when(
        (second(to_timestamp("sunset")) > 30) & (minute(to_timestamp("sunset")) != 59), minute(to_timestamp("sunset")) + 1) \
        .otherwise(minute(to_timestamp("sunset")))

    res = df.withColumnRenamed("temp", "tempavg").withColumn("year", split(col('datetime'), '-')[0]) \
     .withColumn("month", split(col('datetime'), '-')[1]) \
     .withColumn('sunriseHour', round(hour(to_timestamp("sunrise"))+ (sunrise_minute_rounded*0.01),2)) \
     .withColumn('sunsetHour', hour(to_timestamp("sunset"))+ (sunset_minute_rounded*0.01) ) \
     .withColumn('name', name_expr) \
     .withColumn('preciphours', round((col("precipprob")/100)*2,2)) \
     .select(
        "name", "year","datetime","month", "sunriseHour", "sunsetHour",
        "tempmax", "tempmin", "tempavg","precip","preciphours", "snow", 
        "windspeed", "cloudcover"
    )
    return res

spain_df = selColumns(dfSpain)
germany_df = selColumns(dfGermany)

df_total = spain_df.union(germany_df)

def agg_monthly(df_total):
    return df_total.groupBy('name', 'year', 'month').agg(
        round(mean(col('sunriseHour')), 2).alias('avg_sunriseHour'),
        round(mean(col('tempmax')), 2).alias('avg_tempmax')
    ).orderBy('name', 'year', 'month')

final_df = agg_monthly(df_total)