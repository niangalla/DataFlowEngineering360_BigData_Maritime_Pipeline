from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess  # Pour appeler les scripts Python externes

# Fonction pour appeler kpler_producer.py
def run_kpler_producer():
    subprocess.run(['python', 'kpler_producer.py'], check=True)

# Fonction pour appeler openweather_producer.py
def run_openweather_producer():
    subprocess.run(['python', 'openweather_producer.py'], check=True)

# DAG pour polling toutes les 5 min (simule real-time)
dag = DAG(
    'port_producers_polling',
    start_date=datetime(2025, 11, 1),
    schedule_interval=timedelta(minutes=5),  # Polling fréquent - ajuste pour quotas API
    catchup=False
)

kpler_task = PythonOperator(task_id='run_kpler_producer', python_callable=run_kpler_producer, dag=dag)
weather_task = PythonOperator(task_id='run_openweather_producer', python_callable=run_openweather_producer, dag=dag)

# Exécute en parallèle (pas de dépendance)
kpler_task
weather_task