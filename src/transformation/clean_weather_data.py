from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp, date_format
from pyspark.sql.types import DoubleType, IntegerType

# Configuration
APP_NAME = "WeatherDataCleaning"
RAW_ZONE_PATH = "hdfs://namenode:8020/datalake/raw_zone"
CLEAN_ZONE_PATH = "hdfs://namenode:8020/datalake/clean_zone/weather"

def main():
    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("Reading weather data from Raw Zone...")
    
    # Read from both historical and realtime paths if they exist
    try:
        # Read Historical weather data (JSONL files from MySQL/MongoDB)
        df_historical = spark.read.json(f"{RAW_ZONE_PATH}/historical/weather/*/*.jsonl")
        print(f"Read historical weather data count: {df_historical.count()}")
    except Exception as e:
        print(f"Warning: Could not read historical weather data: {e}")
        df_historical = None

    try:
        # Read Realtime weather data (Parquet from Kafka streaming)
        df_realtime = spark.read.parquet(f"{RAW_ZONE_PATH}/realtime/weather")
        print(f"Read realtime weather data count: {df_realtime.count()}")
    except Exception as e:
        print(f"Warning: Could not read realtime weather data: {e}")
        df_realtime = None

    # Combine DataFrames
    if df_historical and df_realtime:
        print("Standardizing schemas...")
        
        # Ensure numeric fields are properly typed
        df_historical = df_historical \
            .withColumn("temperature", col("temperature").cast(DoubleType())) \
            .withColumn("humidity", col("humidity").cast(IntegerType()))
        
        df_realtime = df_realtime \
            .withColumn("temperature", col("temperature").cast(DoubleType())) \
            .withColumn("humidity", col("humidity").cast(IntegerType()))
        
        df_combined = df_historical.unionByName(df_realtime, allowMissingColumns=True)
    elif df_historical:
        df_combined = df_historical \
            .withColumn("temperature", col("temperature").cast(DoubleType())) \
            .withColumn("humidity", col("humidity").cast(IntegerType()))
    elif df_realtime:
        df_combined = df_realtime
    else:
        print("No weather data found in Raw Zone.")
        return

    print(f"Total raw weather records: {df_combined.count()}")

    # Transformations
    print("Applying transformations...")
    
    df_clean = df_combined \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("temperature", col("temperature").cast(DoubleType())) \
        .withColumn("humidity", col("humidity").cast(IntegerType()))

    # Add date column for partitioning based on weather timestamp
    df_clean = df_clean.withColumn(
        "date", 
        date_format(col("timestamp"), "yyyy-MM-dd")
    )

    # Deduplication - assuming timestamp makes a record unique
    df_clean = df_clean.dropDuplicates(["timestamp"])
    
    print(f"Count after deduplication: {df_clean.count()}")

    # Write to Clean Zone with date partitioning
    # Using append mode to add new data incrementally without overwriting existing partitions
    print(f"Writing to {CLEAN_ZONE_PATH}...")
    
    df_clean.write \
        .mode("append") \
        .partitionBy("date") \
        .parquet(CLEAN_ZONE_PATH)

    print("Weather data cleaning completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()
