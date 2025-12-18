from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, date_format, hour
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import argparse

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "weather_realtime"
HDFS_PATH = "hdfs://namenode:8020/datalake/raw_zone/realtime/weather"
CHECKPOINT_PATH = "hdfs://namenode:8020/datalake/checkpoints/realtime_weather_v2"  # Changed to v2 for new partitioning

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--batch', action='store_true', help='Run in batch mode (process available data and exit)')
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("WeatherStreaming") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Define Schema
    schema = StructType([
        StructField("temperature", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("condition", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("source", StringType(), True)
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
    # Use the timestamp from data for partitioning (more accurate than current_timestamp)
    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp_parsed", to_timestamp(col("timestamp"))) \
        .withColumn("date", date_format(col("timestamp_parsed"), "yyyy-MM-dd")) \
        .withColumn("hour", hour(col("timestamp_parsed"))) \
        .drop("timestamp_parsed")  # Remove temporary column

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
        query = writer.trigger(processingTime="30 seconds").start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
