from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import redshift_connector
import boto3

# 기본 설정
default_args = {
    'owner': ['HAKSEONG'],
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 20, 15, 0),  # UTC 15시 = KST 00시
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['koreakseong22@gmail.com'],
}

# S3 데이터를 Redshift로 적재하는 함수
def copy_s3_to_redshift(table_name, base_prefix, execution_date):
    """
    S3 데이터를 Redshift로 COPY.
    """
    # Variables에서 값 가져오기
    bucket_name = Variable.get('S3_SILVER_BASE_PATH')  # S3 Silver Base Path
    iam_role = Variable.get('REDSHIFT_IAM_ROLE')       # IAM Role ARN
    database_name = Variable.get('REDSHIFT_DATABASE')  # Redshift Database
    schema_name = Variable.get('REDSHIFT_SILVER_SCHEMA')  # Redshift Schema

    # Redshift 연결 설정
    redshift_conn = BaseHook.get_connection('redshift-gureum')  # Airflow Connection ID
    conn = redshift_connector.connect(
        host=redshift_conn.host,
        database=database_name,
        user=redshift_conn.login,
        password=redshift_conn.password,
        port=redshift_conn.port
    )

    # 날짜와 시간별 S3 경로 설정
    date_prefix = execution_date.strftime('%Y-%m-%d')
    time_prefix = execution_date.strftime('%H')  # 00-23 시간 값
    prefix = f"{base_prefix}{date_prefix}/{time_prefix}/"

    # S3에서 Parquet 파일 경로 탐색
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    parquet_files = [
        f"s3://{bucket_name}/{obj['Key']}" for obj in response.get('Contents', [])
        if obj['Key'].endswith('.parquet')
    ]

    # Parquet 파일 적재
    if parquet_files:
        # 테이블 생성 쿼리
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            id VARCHAR,
            user_name VARCHAR,
            game_id VARCHAR,
            game_name VARCHAR,
            type VARCHAR,
            title VARCHAR,
            viewer_count INT,
            language VARCHAR,
            is_mature BOOLEAN,
            collected_at TIMESTAMP,
        );
        """
        cursor = conn.cursor()
        cursor.execute(create_table_query)

        # COPY 명령어로 Parquet 파일 적재
        for parquet_file in parquet_files:
            copy_query = f"""
            COPY {schema_name}.{table_name}
            FROM '{parquet_file}'
            IAM_ROLE '{iam_role}'
            FORMAT AS PARQUET;
            """
            cursor.execute(copy_query)
        conn.commit()
        conn.close()
        print(f"Data successfully loaded into {schema_name}.{table_name} from {len(parquet_files)} Parquet files.")
    else:
        print(f"No Parquet files found in {prefix}")

# DAG 정의
dag = DAG(
    'twitch_silver_to_redshift_2',
    default_args=default_args,
    description='S3 데이터를 Redshift로 COPY하는 DAG',
    schedule_interval="0 15,19,23,3,7,11 * * *",  # KST 기준 4시간마다 실행
    catchup=False,
    concurrency=4,
    max_active_runs=4,
    tags=['silver', 'redshift'],
)

# Twitch Streams 데이터를 Redshift로 적재하는 태스크
load_streams_to_redshift = PythonOperator(
    task_id='load_streams_to_redshift',
    python_callable=copy_s3_to_redshift,
    op_args=[
        'twitch_streams',  # 테이블 이름
        Variable.get('S3_SILVER_BASE_PATH') + 'twitch/streams/',  # Base S3 경로
        '{{ execution_date }}'  # Airflow 실행 날짜
    ],
    provide_context=True,
    dag=dag,
)

# Twitch Top Categories 데이터를 Redshift로 적재하는 태스크
load_categories_to_redshift = PythonOperator(
    task_id='load_categories_to_redshift',
    python_callable=copy_s3_to_redshift,
    op_args=[
        'twitch_categories',  # 테이블 이름
        Variable.get('S3_SILVER_BASE_PATH') + 'twitch/top_categories/',  # Base S3 경로
        '{{ execution_date }}'  # Airflow 실행 날짜
    ],
    provide_context=True,
    dag=dag,
)

# DAG 작업 의존성 설정
load_streams_to_redshift
load_categories_to_redshift
