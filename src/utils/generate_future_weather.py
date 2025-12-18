

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine


# MySQL Configuration (hardcoded for standalone script)
MYSQL_HOST = 'mysql'
MYSQL_USER = 'dbuser'
MYSQL_PASSWORD = 'passer123'
MYSQL_DATABASE = 'port_db'
MYSQL_PORT = 3306

def generate_future_weather(start_date, end_date):
    """Generate synthetic weather data for future dates."""
    print(f"Generating weather data from {start_date} to {end_date}")
    
    dates = pd.date_range(start=start_date, end=end_date, freq='H')
    
    # Synthetic data generation
    # Temperature: sinusoidal pattern with daily cycle + random noise
    # Base temp around 25C for Dakar
    day_of_year = dates.dayofyear
    hour = dates.hour
    
    temp = 25 + 5 * np.sin(2 * np.pi * (day_of_year - 80) / 365) + \
           3 * np.sin(2 * np.pi * (hour - 14) / 24) + \
           np.random.normal(0, 1, len(dates))
           
    humidity = 70 + 10 * np.sin(2 * np.pi * (hour - 4) / 24) + \
               np.random.normal(0, 5, len(dates))
    humidity = np.clip(humidity, 40, 100)
    
    # Weather codes (WMO)
    # 0: Clear, 1-3: Cloudy, 51-67: Rain
    # Adjusted for Dakar climate: mostly sunny, some clouds, occasional rain
    weather_codes = np.random.choice(
        [0, 1, 2, 3, 51, 61], 
        size=len(dates), 
        p=[0.50, 0.20, 0.15, 0.10, 0.03, 0.02]  # More variety: 50% sunny, 45% cloudy, 5% rain
    )
    
    # Map weather codes to conditions
    conditions = []
    for code in weather_codes:
        if code == 0:
            conditions.append('Sunny')
        elif code < 4:
            conditions.append('Cloudy')
        else:
            conditions.append('Rain')
            
    df = pd.DataFrame({
        'timestamp': dates,
        'temperature': np.round(temp, 1),
        'humidity': np.round(humidity, 0),
        'condition': conditions
    })
    
    return df

def save_to_mysql(df):
    """Save DataFrame to MySQL."""
    connection_string = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    engine = create_engine(connection_string)
    
    # Rename columns to match MySQL schema if needed
    # Assuming schema: timestamp, temperature, humidity, weather_code
    # Check existing schema first or just map
    
    # Based on fetch_historical_weather.py logic (usually it saves as is or maps)
    # Let's assume the table is 'weather_data'
    
    df_mysql = df.rename(columns={
        'timestamp': 'timestamp',
        'temperature_2m': 'temperature',
        'relative_humidity_2m': 'humidity',
        'weather_code': 'weather_code'
    })
    
    print(f"Saving {len(df)} records to MySQL table 'weather_data'...")
    try:
        df_mysql.to_sql('historical_weather', con=engine, if_exists='append', index=False)
        print("Data saved successfully!")
    except Exception as e:
        print(f"Error saving to MySQL: {e}")

if __name__ == "__main__":
    # Generate for Nov 2025 to Feb 2026
    start = "2025-11-01"
    end = "2026-02-01"
    
    df = generate_future_weather(start, end)
    save_to_mysql(df)
