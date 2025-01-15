from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

default_args = {
    'owner': 'kimhee',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 20, 15, 0),  # UTC 15시 = KST 00시
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['wjdeun0642@gmail.com'],
}

dag = DAG(
    'youtube_silver',
    default_args=default_args,
    description='youtube_bronze_4hourly DAG가 수집한 Raw JSON 데이터를 Parquet 포맷으로 변환하여 S3 버킷에 저장하고, 변환된 Parquet 파일은 Redshift 테이블로 적재',
    schedule_interval=None,
    catchup=False,
    concurrency=4,
    max_active_runs=4,
    tags=['youtube', 'silver'],
)

wait_for_bronze_4hourly = ExternalTaskSensor(
    task_id='wait_for_bronze_4hourly',
    external_dag_id='youtube_bronze_4hourly',
    external_task_id=None,
    mode='reschedule',
    timeout=1800,
    poke_interval=300,
    dag=dag,
)

first_glue_job = GlueJobOperator(
    task_id="run_youtube_glue_job",
    job_name="gureum-youtube-videos",
    region_name="ap-northeast-2",
    aws_conn_id="aws_gureum",
    wait_for_completion=True,
    dag=dag,
)

wait_for_bronze_4hourly >> first_glue_job