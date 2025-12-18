#!/usr/bin/env python3
"""
Script pour ajuster les dates des données de futur vers passé.
Usage: python3 adjust_dates_to_past.py
"""

import sys
sys.path.append('/opt/airflow')

from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import os

# Configuration
MYSQL_HOST = os.getenv('MYSQL_HOST', 'mysql_local')
MYSQL_USER = os.getenv('MYSQL_USER', 'dbuser')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'passer123')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'port_db')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3306))

def adjust_dates_to_past():
    """Ajuste les dates futures vers le passé (derniers 30 jours)."""
    
    connection_string = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    engine = create_engine(connection_string)
    
    print("🔧 Ajustement des dates vers le passé...")
    
    # Stratégie: Soustraire 40 jours de toutes les dates
    # Cela mettra les données de Dec 2025-Jan 2026 vers Nov 2025
    
    with engine.connect() as conn:
        # Vérifier les dates avant
        result = conn.execute(text("""
            SELECT 
                MIN(arrival_time) as min_arr, 
                MAX(arrival_time) as max_arr,
                MIN(departure_time) as min_dep,
                MAX(departure_time) as max_dep
            FROM port_traffic
        """))
        row = result.fetchone()
        print(f"\n📅 Avant ajustement:")
        print(f"   Arrival:   {row[0]} → {row[1]}")
        print(f"   Departure: {row[2]} → {row[3]}")
        
        # Ajuster les dates
        conn.execute(text("""
            UPDATE port_traffic 
            SET 
                arrival_time = DATE_SUB(arrival_time, INTERVAL 40 DAY),
                departure_time = DATE_SUB(departure_time, INTERVAL 40 DAY)
            WHERE arrival_time IS NOT NULL OR departure_time IS NOT NULL
        """))
        conn.commit()
        
        # Vérifier après
        result = conn.execute(text("""
            SELECT 
                MIN(arrival_time) as min_arr, 
                MAX(arrival_time) as max_arr,
                MIN(departure_time) as min_dep,
                MAX(departure_time) as max_dep
            FROM port_traffic
        """))
        row = result.fetchone()
        print(f"\n✅ Après ajustement:")
        print(f"   Arrival:   {row[0]} → {row[1]}")
        print(f"   Departure: {row[2]} → {row[3]}")
        
        print("\n🎉 Dates ajustées avec succès !")
        print("\n⚠️  N'oubliez pas de relancer les pipelines :")
        print("   1. Transformation (Clean Zone)")
        print("   2. Data Warehouse ETL")

if __name__ == "__main__":
    adjust_dates_to_past()
