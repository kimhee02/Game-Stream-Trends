from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

default_args = {
    'owner': 'BEOMJUN',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 20, 15, 0),  # UTC 15시 = KST 00시
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['cbbsjj0314@gmail.com'],
}

dag = DAG(
    'twitch_silver',
    default_args=default_args,
    description='twitch_bronze_4hourly DAG가 수집한 Raw JSON 데이터를 Parquet 포맷으로 변환하여 S3 버킷에 저장하고, 변환된 Parquet 파일은 Redshift 테이블로 적재',
    schedule_interval="0 15 * * *",
    catchup=False,
    concurrency=4,
    max_active_runs=4,
    tags=['twitch', 'silver'],
)

wait_for_bronze_4hourly = ExternalTaskSensor(
    task_id='wait_for_bronze_4hourly',
    external_dag_id='twitch_bronze_4hourly',
    external_task_id=None,
    mode='reschedule',
    timeout=1800,
    poke_interval=300,
    dag=dag,
)

trigger_streams_glue_job = GlueJobOperator(
    task_id="run_streams_glue_job",
    job_name="gureum-twitch-streams",
    region_name="ap-northeast-2",
    aws_conn_id="aws_gureum",
    wait_for_completion=True,
    dag=dag,
)

trigger_top_categories_glue_job = GlueJobOperator(
    task_id="run_top_categories_glue_job",
    job_name="gureum-twitch-top-categories",
    region_name="ap-northeast-2",
    aws_conn_id="aws_gureum",
    wait_for_completion=True,
    dag=dag,
)

wait_for_bronze_4hourly >> (trigger_streams_glue_job, trigger_top_categories_glue_job)