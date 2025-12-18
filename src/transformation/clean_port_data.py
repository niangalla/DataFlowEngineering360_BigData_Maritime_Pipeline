from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp, when, coalesce, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, LongType

# Configuration
APP_NAME = "PortTrafficCleaning"
RAW_ZONE_PATH = "hdfs://namenode:8020/datalake/raw_zone"
CLEAN_ZONE_PATH = "hdfs://namenode:8020/datalake/clean_zone/port_traffic"

def main():
    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("Reading data from Raw Zone...")
    
    # Read from both historical and realtime paths if they exist
    # We use a wildcard to try and catch available data
    # Note: In a real scenario, we might want to be more specific or handle empty paths gracefully
    try:
        # Read Historical (JSONL/CSV converted to JSONL in HDFS)
        # The batch ingestion writes JSONL files
        df_historical = spark.read.json(f"{RAW_ZONE_PATH}/historical/*/*.jsonl")
        print(f"Read historical data count: {df_historical.count()}")
    except Exception as e:
        print(f"Warning: Could not read historical data: {e}")
        df_historical = None

    try:
        # Read Realtime (Parquet) - now partitioned by date/hour
        df_realtime = spark.read.parquet(f"{RAW_ZONE_PATH}/realtime/port_traffic")
        print(f"Read realtime data count: {df_realtime.count()}")
    except Exception as e:
        print(f"Warning: Could not read realtime data: {e}")
        df_realtime = None

    # Combine DataFrames
    if df_historical and df_realtime:
        # Standardize schema before union - convert pilot_required to boolean
        print("Standardizing schemas...")
        
        # Convert pilot_required to boolean in historical data (it's stored as string)
        df_historical = df_historical.withColumn(
            "pilot_required", 
            when(col("pilot_required").cast(StringType()).isin("true", "True", "1"), True)
            .when(col("pilot_required").cast(StringType()).isin("false", "False", "0"), False)
            .otherwise(col("pilot_required").cast(BooleanType()))
        )
        
        # Ensure other numeric fields are properly typed
        for dtype_col in ["cargo_volume", "delay_minutes", "operation_duration", "quay_number"]:
            if dtype_col in df_historical.columns:
                df_historical = df_historical.withColumn(dtype_col, col(dtype_col).cast(LongType()))
            if dtype_col in df_realtime.columns:
                df_realtime = df_realtime.withColumn(dtype_col, col(dtype_col).cast(LongType()))
        
        df_combined = df_historical.unionByName(df_realtime, allowMissingColumns=True)
    elif df_historical:
        # Convert pilot_required to boolean even for historical-only data
        df_historical = df_historical.withColumn(
            "pilot_required", 
            when(col("pilot_required").cast(StringType()).isin("true", "True", "1"), True)
            .when(col("pilot_required").cast(StringType()).isin("false", "False", "0"), False)
            .otherwise(col("pilot_required").cast(BooleanType()))
        )
        df_combined = df_historical
    elif df_realtime:
        df_combined = df_realtime
    else:
        print("No data found in Raw Zone.")
        return

    print(f"Total raw records: {df_combined.count()}")

    # Transformations
    print("Applying transformations...")
    
    df_clean = df_combined \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("arrival_time", to_timestamp(col("arrival_time"))) \
        .withColumn("departure_time", to_timestamp(col("departure_time"))) \
        .withColumn("draft_depth", col("draft_depth").cast(DoubleType())) \
        .withColumn("cargo_volume", col("cargo_volume").cast(LongType())) \
        .withColumn("imo_number", col("imo_number").cast(LongType()))

    # Add date column for partitioning based on event time (arrival_time or departure_time)
    df_clean = df_clean.withColumn(
        "date", 
        date_format(
            coalesce(col("arrival_time"), col("departure_time"), current_timestamp()),
            "yyyy-MM-dd"
        )
    )

    # Deduplication
    # Assuming (imo_number, arrival_time) or (vessel_id, timestamp) makes a record unique
    # For simplicity, let's deduplicate by vessel_id and the event time (arrival or departure)
    # If arrival_time is null, use departure_time
    df_clean = df_clean.dropDuplicates(["vessel_id", "arrival_time", "departure_time", "status"])
    
    print(f"Count after deduplication: {df_clean.count()}")

    # Write to Clean Zone with date partitioning
    # Using append mode to add new data incrementally without overwriting existing partitions
    print(f"Writing to {CLEAN_ZONE_PATH}...")
    
    df_clean.write \
        .mode("append") \
        .partitionBy("date") \
        .parquet(CLEAN_ZONE_PATH)

    print("Data cleaning completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()
