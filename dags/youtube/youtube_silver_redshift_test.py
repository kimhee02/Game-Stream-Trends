from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

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

# Glue 작업 태스크
first_glue_job = GlueJobOperator(
    task_id="run_youtube_glue_job",
    job_name="gureum-youtube-videos",
    region_name="ap-northeast-2",
    aws_conn_id="aws_gureum",
    wait_for_completion=True,
    dag=dag,
)

# Redshift COPY 작업 태스크
copy_to_redshift = PostgresOperator(
    task_id="copy_to_redshift",
    postgres_conn_id="redshift_default",  # Airflow에서 설정된 Redshift 연결 ID
    sql="""
        COPY test
        FROM 's3://gureum-bucket/data/processed/silver/youtube/videos/'
        IAM_ROLE 'arn:aws:iam::862327261051:role/gureum-redshift-role'
        FORMAT AS PARQUET;
    """,
    dag=dag,
)

# 태스크 의존성 정의
first_glue_job >> copy_to_redshift