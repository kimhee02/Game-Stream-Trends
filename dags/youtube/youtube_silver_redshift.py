from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import timedelta

# 기본 설정
default_args = {
    'owner': 'kimhee',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['wjdeun0642@gmail.com'],
}

# Airflow Variables 가져오기
REDSHIFT_DATABASE = Variable.get('REDSHIFT_DATABASE')
REDSHIFT_SILVER_SCHEMA = Variable.get('REDSHIFT_SILVER_SCHEMA')
S3_SILVER_BASE_PATH = Variable.get('S3_SILVER_BASE_PATH')
REDSHIFT_IAM_ROLE = Variable.get('REDSHIFT_IAM_ROLE')
S3_BUCKET_NAME = Variable.get('S3_BUCKET_NAME')

# S3에서 유효한 시간 경로를 찾는 함수
def find_valid_hour(**kwargs):
    s3 = S3Hook(aws_conn_id='aws_gureum')
    bucket_name = S3_BUCKET_NAME
    base_path = S3_SILVER_BASE_PATH
    execution_date = kwargs['ds']
    
    for hour in range(24):
        key = f"{base_path}/youtube/videos/{execution_date}/{hour:02d}/_SUCCESS"
        if s3.check_for_key(key, bucket_name=bucket_name):
            return f"{base_path}/youtube/videos/{execution_date}/{hour:02d}/"
    
    raise ValueError(f"No valid hour found for {execution_date}")

# DAG 정의
with DAG(
    dag_id='silver_youtube_videos_to_redshift',
    default_args=default_args,
    description='S3에 있는 YouTube videos silver layer를 redshift로 COPY',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # 유효한 시간 경로 찾기
    task_find_valid_hour = PythonOperator(
        task_id='find_valid_hour',
        python_callable=find_valid_hour,
        provide_context=True,
    )

    # Redshift로 데이터 적재
    task_copy_to_redshift = SQLExecuteQueryOperator(
        task_id='load_silver_to_redshift',
        sql=f"""
            COPY {{ params.schema }}.youtube_videos
            FROM 's3://{S3_BUCKET_NAME}/{{{{ ti.xcom_pull(task_ids='find_valid_hour') }}}}'
            IAM_ROLE '{{{{ params.iam_role }}}}'
            FORMAT AS PARQUET;
        """,
        params={
            "schema": REDSHIFT_SILVER_SCHEMA,
            "iam_role": REDSHIFT_IAM_ROLE,
        },
        conn_id='redshift-gureum',
    )

    # 태스크 의존성 정의
    task_find_valid_hour >> task_copy_to_redshift
