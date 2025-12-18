-- =============================================================================
-- DataFlow 360 - Star Schema Implementation
-- Context: Port Autonome de Dakar Traffic Analysis
-- =============================================================================

-- 1. CLEANUP: Drop old tables if they exist
DROP TABLE IF EXISTS fact_port_traffic_daily CASCADE;
DROP TABLE IF EXISTS fact_weather_daily CASCADE;

-- Drop new tables if they exist (for idempotency)
DROP TABLE IF EXISTS fact_port_events CASCADE;
DROP TABLE IF EXISTS dim_weather CASCADE;
DROP TABLE IF EXISTS dim_terminal CASCADE;
DROP TABLE IF EXISTS dim_vessel CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;

-- 2. DIMENSIONS

-- Dimension: Date (Time Intelligence)
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,        -- Format: YYYYMMDD
    full_date DATE NOT NULL,
    year INT,
    month INT,
    day INT,
    day_of_week INT,                 -- 1=Monday, 7=Sunday
    day_name VARCHAR(20),
    month_name VARCHAR(20),
    is_weekend BOOLEAN,
    season VARCHAR(20)               -- 'Hivernage' (Jun-Oct), 'Saison Seche' (Nov-May)
);

-- Dimension: Vessel (Navires)
CREATE TABLE dim_vessel (
    vessel_key SERIAL PRIMARY KEY,
    imo_number BIGINT UNIQUE,        -- Natural Key
    vessel_name VARCHAR(255),
    vessel_type VARCHAR(100),
    flag VARCHAR(100),
    length DOUBLE PRECISION,
    width DOUBLE PRECISION,
    gross_tonnage INT
);

-- Dimension: Terminal (Lieux)
CREATE TABLE dim_terminal (
    terminal_key SERIAL PRIMARY KEY,
    terminal_name VARCHAR(255) UNIQUE, -- e.g., 'DP WORLD DAKAR', 'MOLE 1'
    port_zone VARCHAR(100),
    operator VARCHAR(100)
);

-- Dimension: Weather (Conditions Météo)
-- Linked to the event date/time
CREATE TABLE dim_weather (
    weather_key SERIAL PRIMARY KEY,
    date DATE,
    avg_temp_celsius DOUBLE PRECISION,
    condition VARCHAR(100),          -- e.g., 'Sunny', 'Rain'
    humidity_percent INT,
    wind_speed_kmh DOUBLE PRECISION,
    UNIQUE(date)                     -- One weather record per day (simplified for daily analysis)
);

-- 3. FACT TABLE

-- Fact: Port Events (Arrivées/Départs)
CREATE TABLE fact_port_events (
    event_id SERIAL PRIMARY KEY,
    date_key INT REFERENCES dim_date(date_key),
    vessel_key INT REFERENCES dim_vessel(vessel_key),
    terminal_key INT REFERENCES dim_terminal(terminal_key),
    weather_key INT REFERENCES dim_weather(weather_key),
    
    -- Metrics
    cargo_volume BIGINT,
    delay_minutes INT,
    operation_duration_hours DOUBLE PRECISION,
    
    -- Attributes
    traffic_type VARCHAR(50),        -- 'Arrival', 'Departure'
    status VARCHAR(50),              -- 'Scheduled', 'Arrived', 'Departed'
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. INDEXES (Performance)
CREATE INDEX idx_fact_date ON fact_port_events(date_key);
CREATE INDEX idx_fact_vessel ON fact_port_events(vessel_key);
CREATE INDEX idx_fact_terminal ON fact_port_events(terminal_key);
