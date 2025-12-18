from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SchemaDebug") \
    .getOrCreate()

print("=" * 80)
print("INSPECTING: Clean Zone Port Traffic")
print("=" * 80)

try:
    df_port = spark.read.parquet("hdfs://namenode:8020/datalake/clean_zone/port_traffic")
    print("\nSchema:")
    df_port.printSchema()
    print(f"\nTotal records: {df_port.count()}")
    print("\nColumn names:")
    print(df_port.columns)
    print("\nSample (2 rows):")
    df_port.show(2, truncate=False)
    
    # Check for specific columns expected by aggregate script
    print("\nChecking expected columns:")
    expected_cols = ["imo_number", "vessel_name", "vessel_type", "flag", "terminal", "port_zone"]
    for col_name in expected_cols:
        has_col = col_name in df_port.columns
        print(f"  {col_name}: {'✓' if has_col else '✗ MISSING'}")
        
except Exception as e:
    print(f"ERROR reading port_traffic: {e}")

print("\n" + "=" * 80)
print("INSPECTING: Clean Zone Weather")
print("=" * 80)

try:
    df_weather = spark.read.parquet("hdfs://namenode:8020/datalake/clean_zone/weather")
    print("\nSchema:")
    df_weather.printSchema()
    print(f"\nTotal records: {df_weather.count()}")
    print("\nColumn names:")
    print(df_weather.columns)
    print("\nSample (2 rows):")
    df_weather.show(2, truncate=False)
    
except Exception as e:
    print(f"ERROR reading weather: {e}")

spark.stop()
