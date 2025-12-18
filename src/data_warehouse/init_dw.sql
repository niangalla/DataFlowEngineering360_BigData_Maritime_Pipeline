-- Create Port Traffic Daily Facts Table
CREATE TABLE IF NOT EXISTS fact_port_traffic_daily (
    date DATE PRIMARY KEY,
    total_arrivals INT,
    total_departures INT,
    avg_delay_minutes FLOAT,
    total_cargo_volume BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Weather Daily Facts Table
CREATE TABLE IF NOT EXISTS fact_weather_daily (
    date DATE PRIMARY KEY,
    avg_temperature FLOAT,
    min_temperature FLOAT,
    max_temperature FLOAT,
    avg_humidity INT,
    condition_dominante VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Indexes for performance
CREATE INDEX idx_port_date ON fact_port_traffic_daily(date);
CREATE INDEX idx_weather_date ON fact_weather_daily(date);
