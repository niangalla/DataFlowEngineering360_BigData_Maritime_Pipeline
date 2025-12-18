from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("DebugCleanWeather").getOrCreate()

df = spark.read.parquet("hdfs://namenode:8020/datalake/clean_zone/weather")
print(f"Total records: {df.count()}")
print("Sample records:")
df.select("timestamp", "date", "temperature", "condition").show(20, truncate=False)

print("Distinct dates:")
df.select("date").distinct().show(100, truncate=False)

print("Distinct timestamps (first 20):")
df.select("timestamp").distinct().show(20, truncate=False)
