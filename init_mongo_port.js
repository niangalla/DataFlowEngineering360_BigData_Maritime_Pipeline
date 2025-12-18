db = db.getSiblingDB('port_db');
db.createCollection("port_traffic");

db.createCollection("historical_weather")
db.port_traffic.createIndex({ vessel_id: 1 }, { unique: true });  // Idempotent unique key
db.historical_weather.createIndex({ timestamp: 1 }, {unique: true});