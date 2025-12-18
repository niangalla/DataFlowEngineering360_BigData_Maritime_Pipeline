CREATE TABLE IF NOT EXISTS historical_weather (
    timestamp DATETIME PRIMARY KEY,
    temperature FLOAT,
    humidity FLOAT,
    `condition` VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS port_traffic (
  vessel_id VARCHAR(36) PRIMARY KEY,
  vessel_name VARCHAR(255),
  imo_number INT,
  flag VARCHAR(10),
  vessel_type VARCHAR(50),
  draft_depth FLOAT,
  arrival_time DATETIME,
  departure_time DATETIME,
  port_zone VARCHAR(50),
  terminal VARCHAR(100),
  quay_number INT,
  pilot_required BOOLEAN,
  status VARCHAR(50),
  cargo_type VARCHAR(50),
  cargo_volume INT,
  operation_duration INT,
  delay_minutes INT,
  weather_condition VARCHAR(50)
);
