from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess
import sys
from pathlib import Path

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_pipeline_v2():
    script_path = Path(__file__).parent / 'weather_pipeline_v2.py'
    
    if not script_path.exists():
        raise Exception(f"Файл не найден: {script_path}")
    
    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True
    )
    
    print(result.stdout)
    if result.stderr:
        print("Ошибки:")
        print(result.stderr)
    
    if result.returncode != 0:
        raise Exception(f"Скрипт завершился с кодом {result.returncode}")

with DAG(
    'weather_pipeline_v2',
    default_args=default_args,
    description='Расширенный сбор погоды (несколько городов + прогноз)',
    schedule_interval='@hourly',
    catchup=False,
    tags=['weather', 'etl', 'advanced'],
) as dag:
    
    run_task = PythonOperator(
        task_id='run_weather_pipeline_v2',
        python_callable=run_pipeline_v2,
    )