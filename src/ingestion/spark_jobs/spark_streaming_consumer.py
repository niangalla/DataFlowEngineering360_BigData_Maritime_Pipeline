from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, date_format, hour
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, LongType

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092" # Internal docker network address
KAFKA_TOPIC = "port_traffic"
HDFS_PATH = "hdfs://namenode:8020/datalake/raw_zone/realtime/port_traffic"
CHECKPOINT_PATH = "hdfs://namenode:8020/datalake/checkpoints/realtime_port_v2"  # Changed to v2 for new partitioning

import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--batch', action='store_true', help='Run in batch mode (process available data and exit)')
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("PortTrafficStreaming") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Define Schema (Must match the Producer output)
    schema = StructType([
        StructField("vessel_id", StringType(), True),
        StructField("vessel_name", StringType(), True),
        StructField("imo_number", LongType(), True),
        StructField("flag", StringType(), True),
        StructField("vessel_type", StringType(), True),
        StructField("draft_depth", DoubleType(), True),
        StructField("arrival_time", StringType(), True),
        StructField("departure_time", StringType(), True),
        StructField("port_zone", StringType(), True),
        StructField("terminal", StringType(), True),
        StructField("quay_number", IntegerType(), True),
        StructField("pilot_required", BooleanType(), True),
        StructField("status", StringType(), True),
        StructField("cargo_type", StringType(), True),
        StructField("cargo_volume", IntegerType(), True),
        StructField("operation_duration", IntegerType(), True),
        StructField("delay_minutes", IntegerType(), True),
        StructField("weather_condition", StringType(), True),
        StructField("sealine", StringType(), True),
        StructField("sealine_name", StringType(), True),
        StructField("service_code", StringType(), True),
        StructField("service_name", StringType(), True),
        StructField("traffic_type", StringType(), True),
        StructField("ingestion_timestamp", StringType(), True)
    ])

    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse JSON and add partitioning columns
    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("date", date_format(current_timestamp(), "yyyy-MM-dd")) \
        .withColumn("hour", hour(current_timestamp()))

    # Write to HDFS (Parquet) with date/hour partitioning
    writer = parsed_df \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .partitionBy("date", "hour") \
        .option("path", HDFS_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH)

    if args.batch:
        print("Running in Batch Mode (AvailableNow)")
        query = writer.trigger(availableNow=True).start()
    else:
        print("Running in Continuous Mode")
        query = writer.trigger(processingTime="10 seconds").start()

    query.awaitTermination()

if __name__ == "__main__":
    main()