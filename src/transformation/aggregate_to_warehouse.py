from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, date_format, year, month, dayofmonth, dayofweek, 
    when, lit, count, avg, sum, min, max, trim, lower, row_number, coalesce, round
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType
import psycopg2
from datetime import datetime

# ==============================================================================
# CONFIGURATION
# ==============================================================================
APP_NAME = "StarSchemaETL"
CLEAN_ZONE_PORT = "hdfs://namenode:8020/datalake/clean_zone/port_traffic"
CLEAN_ZONE_WEATHER = "hdfs://namenode:8020/datalake/clean_zone/weather"

# PostgreSQL Connection
DB_HOST = "postgres_dw"
DB_PORT = "5432"
DB_NAME = "data_warehouse"
DB_USER = "warehouse_user"
DB_PASSWORD = "warehouse_password"
POSTGRES_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
POSTGRES_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )

def execute_query(query, params=None):
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(query, params)
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"Error executing query: {e}")
        conn.rollback()
    finally:
        conn.close()

def truncate_table(table_name):
    """Truncate a table (Use with caution!)"""
    print(f"Truncating table {table_name}...")
    execute_query(f"TRUNCATE TABLE {table_name} CASCADE")

# ==============================================================================
# DIMENSION LOADING
# ==============================================================================

def load_dim_date(spark, start_date='2024-01-01', end_date='2026-12-31'):
    """
    Generates and loads the Date Dimension.
    Simple approach: Generate a range of dates using Spark.
    """
    print("Loading Dimension: dim_date...")
    
    # Check if dates already exist to avoid re-inserting everything
    # For simplicity in this demo, we assume we can just insert missing or ignore
    # But since we don't have 'INSERT IGNORE' in Postgres standard JDBC easily without custom query,
    # we will use a naive approach: Generate DF, filter out existing, insert new.
    
    # Generate sequence of dates
    df_dates = spark.sql(f"""
        SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as full_date
    """)
    
    df_dim_date = df_dates.select(
        (year("full_date") * 10000 + month("full_date") * 100 + dayofmonth("full_date")).alias("date_key"),
        col("full_date"),
        year("full_date").alias("year"),
        month("full_date").alias("month"),
        dayofmonth("full_date").alias("day"),
        dayofweek("full_date").alias("day_of_week"),
        date_format("full_date", "EEEE").alias("day_name"),
        date_format("full_date", "MMMM").alias("month_name"),
        when(dayofweek("full_date").isin(1, 7), True).otherwise(False).alias("is_weekend"), # 1=Sun, 7=Sat in Spark usually, let's check standard
        when(month("full_date").between(6, 10), "Hivernage").otherwise("Saison Seche").alias("season")
    )
    
    # Write to DB (Append mode, but we should handle duplicates)
    # Strategy: Read existing keys, filter, write new.
    try:
        existing_keys = spark.read.jdbc(POSTGRES_URL, "dim_date", properties=POSTGRES_PROPERTIES).select("date_key")
        new_dates = df_dim_date.join(existing_keys, "date_key", "left_anti")
        
        if new_dates.count() > 0:
            new_dates.write.jdbc(POSTGRES_URL, "dim_date", "append", POSTGRES_PROPERTIES)
            print(f"Inserted {new_dates.count()} new dates into dim_date.")
        else:
            print("dim_date is up to date.")
            
    except Exception as e:
        # If table is empty or read fails, try writing all
        print(f"Initial load or error reading dim_date: {e}")
        df_dim_date.write.jdbc(POSTGRES_URL, "dim_date", "append", POSTGRES_PROPERTIES)

def load_dim_vessel(spark, df_port):
    """
    Loads Dimension: Vessel
    """
    print("Loading Dimension: dim_vessel...")
    
    # Extract unique vessels from source
    source_vessels = df_port.select(
        col("imo_number"),
        col("vessel_name"),
        col("vessel_type"),
        col("flag"),
        col("length").cast(DoubleType()), # Assuming these columns exist or are null
        col("width").cast(DoubleType()),  # Assuming these columns exist or are null
        lit(None).cast(IntegerType()).alias("gross_tonnage") # Placeholder
    ).distinct().where(col("imo_number").isNotNull())
    
    # Check if table exists and has data
    try:
        existing_vessels = spark.read.jdbc(POSTGRES_URL, "dim_vessel", properties=POSTGRES_PROPERTIES)
        existing_count = existing_vessels.count()
        
        if existing_count == 0:
            # Table is empty, load all source vessels
            source_vessels.write.jdbc(POSTGRES_URL, "dim_vessel", "append", POSTGRES_PROPERTIES)
            print(f"Inserted {source_vessels.count()} new vessels (initial load).")
        else:
            # Deduplicate
            new_vessels = source_vessels.join(existing_vessels, "imo_number", "left_anti")
            if new_vessels.count() > 0:
                new_vessels.write.jdbc(POSTGRES_URL, "dim_vessel", "append", POSTGRES_PROPERTIES)
                print(f"Inserted {new_vessels.count()} new vessels.")
            else:
                print("No new vessels to insert.")
    except Exception as e:
        # Table doesn't exist, create and load
        print(f"Table doesn't exist, creating: {e}")
        source_vessels.write.jdbc(POSTGRES_URL, "dim_vessel", "append", POSTGRES_PROPERTIES)
        print(f"Inserted {source_vessels.count()} new vessels (initial load).")

def load_dim_terminal(spark, df_port):
    """
    Loads Dimension: Terminal
    """
    print("Loading Dimension: dim_terminal...")
    
    # Extract unique terminals
    # Note: 'terminal' column in source might be null or dirty
    source_terminals = df_port.select(
        col("terminal").alias("terminal_name"),
        col("port_zone"),
        lit("Port Autonome de Dakar").alias("operator") # Default
    ).distinct().where(col("terminal_name").isNotNull())
    
    try:
        existing_terminals = spark.read.jdbc(POSTGRES_URL, "dim_terminal", properties=POSTGRES_PROPERTIES)
        existing_count = existing_terminals.count()
        
        if existing_count == 0:
            source_terminals.write.jdbc(POSTGRES_URL, "dim_terminal", "append", POSTGRES_PROPERTIES)
            print(f"Inserted {source_terminals.count()} new terminals (initial load).")
        else:
            new_terminals = source_terminals.join(existing_terminals, "terminal_name", "left_anti")
            if new_terminals.count() > 0:
                new_terminals.write.jdbc(POSTGRES_URL, "dim_terminal", "append", POSTGRES_PROPERTIES)
                print(f"Inserted {new_terminals.count()} new terminals.")
            else:
                print("No new terminals to insert.")
    except Exception as e:
        print(f"Terminal table doesn't exist, creating: {e}")
        source_terminals.write.jdbc(POSTGRES_URL, "dim_terminal", "append", POSTGRES_PROPERTIES)
        print(f"Inserted {source_terminals.count()} new terminals (initial load).")

def load_dim_weather(spark):
    """
    Loads Dimension: Weather (Daily)
    """
    print("Loading Dimension: dim_weather...")
    
    df_weather = spark.read.parquet(CLEAN_ZONE_WEATHER)
    
    # Aggregate to daily level
    # We need one row per day for the dimension
    daily_weather = df_weather.withColumn("date", to_date(col("timestamp"))) \
        .groupBy("date") \
        .agg(
            round(avg("temperature"), 2).alias("avg_temp_celsius"),
            max("condition").alias("condition"), # Simplified
            round(avg("humidity"), 2).alias("humidity_percent"),
            lit(None).cast(DoubleType()).alias("wind_speed_kmh") # Column missing in source
        )
    
    try:
        existing_weather = spark.read.jdbc(POSTGRES_URL, "dim_weather", properties=POSTGRES_PROPERTIES)
        existing_count = existing_weather.count()
        
        if existing_count == 0:
            daily_weather.write.jdbc(POSTGRES_URL, "dim_weather", "append", POSTGRES_PROPERTIES)
            print(f"Inserted {daily_weather.count()} new weather records (initial load).")
        else:
            new_weather = daily_weather.join(existing_weather, "date", "left_anti")
            if new_weather.count() > 0:
                new_weather.write.jdbc(POSTGRES_URL, "dim_weather", "append", POSTGRES_PROPERTIES)
                print(f"Inserted {new_weather.count()} new weather records.")
            else:
                print("No new weather records to insert.")
    except Exception as e:
        print(f"Weather table doesn't exist, creating: {e}")
        daily_weather.write.jdbc(POSTGRES_URL, "dim_weather", "append", POSTGRES_PROPERTIES)
        print(f"Inserted {daily_weather.count()} new weather records (initial load).")

# ==============================================================================
# FACT LOADING
# ==============================================================================

def load_fact_port_events(spark):
    print("Loading Fact Table: fact_port_events...")
    
    # 1. Read Source Data
    df_port = spark.read.parquet(CLEAN_ZONE_PORT)
    
    # Ensure necessary columns for joining - FORCE DATE TYPE
    df_port = df_port.withColumn("event_date", to_date(coalesce(col("arrival_time"), col("departure_time"))))
    df_port = df_port.withColumn("date_key", 
        (year("event_date") * 10000 + month("event_date") * 100 + dayofmonth("event_date"))
    )
    
    # 2. Read Dimensions (to get Surrogate Keys)
    dim_vessel = spark.read.jdbc(POSTGRES_URL, "dim_vessel", properties=POSTGRES_PROPERTIES) \
        .select("vessel_key", "imo_number")
        
    dim_terminal = spark.read.jdbc(POSTGRES_URL, "dim_terminal", properties=POSTGRES_PROPERTIES) \
        .select("terminal_key", "terminal_name")
        
    dim_weather = spark.read.jdbc(POSTGRES_URL, "dim_weather", properties=POSTGRES_PROPERTIES) \
        .select("weather_key", "date")
    
    # 3. Join Source with Dimensions
    # Join Vessel
    df_fact = df_port.join(dim_vessel, df_port.imo_number == dim_vessel.imo_number, "left")
    
    # Join Terminal
    df_fact = df_fact.join(dim_terminal, df_port.terminal == dim_terminal.terminal_name, "left")
    
    # Join Weather
    df_fact = df_fact.join(dim_weather, df_port.event_date == dim_weather.date, "left")
    
    # 4. Select and Rename Columns for Fact Table
    df_final = df_fact.select(
        col("date_key"),
        col("vessel_key"),
        col("terminal_key"),
        col("weather_key"),
        col("cargo_volume"),
        col("delay_minutes"),
        col("operation_duration").alias("operation_duration_hours"), # Assuming it's hours or convert
        trim(lower(col("traffic_type"))).alias("traffic_type"),
        col("status")
    )
    
    # 5. Idempotency: Delete existing events for the processed dates
    # Get unique dates from the current batch
    processed_dates = df_final.select("date_key").distinct().collect()
    date_keys = [str(row.date_key) for row in processed_dates if row.date_key]
    
    if date_keys:
        print(f"Clearing existing facts for {len(date_keys)} dates...")
        keys_str = ",".join(date_keys)
        execute_query(f"DELETE FROM fact_port_events WHERE date_key IN ({keys_str})")
    
    # 6. Write to DB
    print("Writing to fact_port_events...")
    df_final.write.jdbc(POSTGRES_URL, "fact_port_events", "append", POSTGRES_PROPERTIES)
    print("Fact Table Loaded Successfully.")

# ==============================================================================
# MAIN
# ==============================================================================

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # 1. Load Dimensions
    load_dim_date(spark)
    
    # Read source once to pass to dim loaders (optimization)
    try:
        df_port_source = spark.read.parquet(CLEAN_ZONE_PORT)
        
        # Handle missing columns if any (robustness)
        if "length" not in df_port_source.columns:
            df_port_source = df_port_source.withColumn("length", lit(None))
        if "width" not in df_port_source.columns:
            df_port_source = df_port_source.withColumn("width", lit(None))
            
        load_dim_vessel(spark, df_port_source)
        load_dim_terminal(spark, df_port_source)
        load_dim_weather(spark)
        
        # 2. Load Fact
        load_fact_port_events(spark)
        
    except Exception as e:
        print(f"Critical Error in ETL Pipeline: {e}")
    
    spark.stop()
