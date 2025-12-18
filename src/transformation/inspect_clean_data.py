from pyspark.sql import SparkSession
from pyspark.sql.functions import col

CLEAN_ZONE_PORT = "hdfs://namenode:8020/datalake/clean_zone/port_traffic"

def inspect_data():
    spark = SparkSession.builder.appName("InspectCleanData").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        df = spark.read.parquet(CLEAN_ZONE_PORT)
        print("Schema:")
        df.printSchema()
        
        print("Distinct Traffic Type Values:")
        df.select("traffic_type").distinct().show(truncate=False)
        
        print("Sample Data:")
        df.show(5)
        
    except Exception as e:
        print(f"Error reading data: {e}")
        
    spark.stop()

if __name__ == "__main__":
    inspect_data()
