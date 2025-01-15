from airflow import DAG
from airflow.operators.python import PythonOperator
#from scripts.youtube import fetch_videos  # 스크립트에서 함수 호출
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import sys
import os

script_path = os.path.join(os.path.dirname(__file__), '../../scripts/youtube')
sys.path.insert(0, script_path)
from fetch_videos import fetch_youtube_data

default_args = {
    'owner': 'kimhee',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 1, 1, 15, 0),
}

dag = DAG(
    'youtube_bronze_4hourly',
    default_args=default_args,
    description='Fetch YouTube data and save to bronze_json (Bronze Layer)',
    schedule_interval="0 15,19,23,3,7,11 * * *",
    catchup=False,
    tags=['youtube', 'bronze', '4-hourly'],
)

fetch_task = PythonOperator(
    task_id='fetch_youtube_data',
    python_callable=fetch_youtube_data,  # 스크립트의 함수 호출
    dag=dag,
)

# Silver DAG 트리거 작업 추가
trigger_silver_dag = TriggerDagRunOperator(
    task_id='trigger_silver_dag',
    trigger_dag_id='youtube_silver',  # Silver DAG의 ID와 동일해야 함
    wait_for_completion=False,  # Silver DAG이 완료될 때까지 기다리지 않음
    dag=dag,
)

# 의존성 설정
fetch_task >> trigger_silver_dag