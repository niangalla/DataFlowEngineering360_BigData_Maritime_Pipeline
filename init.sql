CREATE DATABASE IF NOT EXISTS sports_db;
USE sports_db;

CREATE TABLE IF NOT EXISTS sportifs (
  sportif_id VARCHAR(20) PRIMARY KEY,
  name VARCHAR(200),
  age INT,
  sex CHAR(1),
  team VARCHAR(100),
  sport VARCHAR(100),
  weight_kg FLOAT,
  height_cm INT,
  level VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS seances (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  sportif_id VARCHAR(20),
  seance_id VARCHAR(50),
  timestamp DATETIME,
  seance_type VARCHAR(50),
  duration_sec INT,
  distance_km FLOAT,
  avg_speed_kmh FLOAT,
  max_speed_kmh FLOAT,
  avg_heart_rate INT,
  max_heart_rate INT,
  calories INT,
  lat DOUBLE,
  lon DOUBLE,
  fatigue_score FLOAT,
  injury_flag BOOLEAN,
  FOREIGN KEY (sportif_id) REFERENCES sportifs(sportif_id)
);