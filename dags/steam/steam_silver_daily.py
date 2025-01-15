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
    'steam_silver_daily',
    default_args=default_args,
    description='steam daily DAG가 수집한 Raw JSON 데이터를 Parquet 포맷으로 변환하여 S3 버킷에 저장하고, 변환된 Parquet 파일은 Redshift 테이블로 적재',
    schedule_interval="0 15 * * *",
    catchup=False,
    concurrency=4,
    max_active_runs=4,
    tags=['steam', 'silver', 'daily'],
)

wait_for_bronze_daily = ExternalTaskSensor(
    task_id='wait_for_bronze_daily',
    external_dag_id='steam_bronze_daily',
    external_task_id=None,
    mode='reschedule',
    timeout=1800,
    poke_interval=300,
    dag=dag,
)

trigger_glue_job_review_metas = GlueJobOperator(
    task_id="run_review_metas_glue_job",
    job_name="gureum-steam-review-metas-in-use",
    region_name="ap-northeast-2",
    aws_conn_id="aws_gureum",
    wait_for_completion=True,
    dag=dag,
)

wait_for_bronze_daily >> trigger_glue_job_review_metas
