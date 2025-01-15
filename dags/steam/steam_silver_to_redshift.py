from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.models import Variable
from datetime import datetime, timedelta
import logging

TABLE_NAME = 'steam_players'
STAGING_TABLE_NAME = f'{TABLE_NAME}_staging'

REDSHIFT_SILVER_SCHEMA = Variable.get('REDSHIFT_SILVER_SCHEMA')
REDSHIFT_IAM_ROLE = Variable.get('REDSHIFT_IAM_ROLE')
S3_SILVER_BASE_PATH = Variable.get('S3_SILVER_BASE_PATH')
S3_BUCKET_NAME = Variable.get('S3_BUCKET_NAME')

default_args = {
    'owner': 'BEOMJUN',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2024, 12, 20, 15, 0),  # UTC 15시 = KST 00시
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['cbbsjj0314@gmail.com'],
}

def generate_valid_s3_paths(execution_date):
    logging.info("Generating valid S3 paths...")
    s3_hook = S3Hook(aws_conn_id='aws_gureum')
    valid_paths = []

    target_date = execution_date - timedelta(days=1)

    for hour in range(24):
        s3_path = f"{S3_SILVER_BASE_PATH}/steam/players/{target_date.strftime('%Y-%m-%d')}/{hour:02d}/"
        objects = s3_hook.list_keys(bucket_name=S3_BUCKET_NAME, prefix=s3_path)
        parquet_files = [obj for obj in objects if obj.endswith('.parquet')] if objects else []
        if parquet_files:
            valid_paths.append(s3_path)
    logging.info(f"Valid S3 paths for {target_date.strftime('%Y-%m-%d')}: {valid_paths}")
    return valid_paths

with DAG(
    dag_id='steam_silver_to_redshift',
    default_args=default_args,
    description='전날 S3에 적재된 Silver Layer를 Redshift로 COPY (steam_players)',
    schedule_interval=None,
    catchup=False,
    tags=['steam', 'silver'],
) as dag:
    
    wait_for_daily_dag = ExternalTaskSensor(
        task_id='wait_for_daily_dag',
        external_dag_id='steam_silver_daily',
        external_task_id=None,
        mode='reschedule',
        timeout=3600,
        poke_interval=3600,
    )    

    wait_for_4hourly_dag = ExternalTaskSensor(
        task_id='wait_for_4hourly_dag',
        external_dag_id='steam_silver_4hourly',
        external_task_id=None,
        mode='reschedule',
        timeout=3600,
        poke_interval=3600,
    )

    s3_paths = generate_valid_s3_paths('{{ execution_date }}')

    drop_staging_table = SQLExecuteQueryOperator(
        task_id='drop_staging_table',
        conn_id='redshift-gureum',
        sql=f"DROP TABLE IF EXISTS {REDSHIFT_SILVER_SCHEMA}.{STAGING_TABLE_NAME};",
    )
    create_staging_table = SQLExecuteQueryOperator(
        task_id='create_staging_table',
        conn_id='redshift-gureum',
        sql=f"""
            CREATE TABLE IF NOT EXISTS {REDSHIFT_SILVER_SCHEMA}.{STAGING_TABLE_NAME} (
                app_id VARCHAR(255),
                player_count BIGINT,
                result BIGINT,
                collected_at TIMESTAMP
            );
        """,
    )

    copy_tasks = []
    
    for s3_path in s3_paths:
        copy_to_staging = SQLExecuteQueryOperator(
            task_id=f'copy_to_staging_{s3_path.split("/")[-2]}_{s3_path.split("/")[-1]}',
            conn_id='redshift-gureum',
            sql=f"""
                COPY {REDSHIFT_SILVER_SCHEMA}.{STAGING_TABLE_NAME} (app_id, player_count, result, collected_at)
                FROM 's3://{S3_BUCKET_NAME}/{s3_path}'
                CREDENTIALS 'aws_iam_role={REDSHIFT_IAM_ROLE}'
                FORMAT AS PARQUET;
            """,
        )
        [wait_for_daily_dag, wait_for_4hourly_dag] >> drop_staging_table >> create_staging_table >> copy_to_staging
        copy_tasks.append(copy_to_staging)

    merge_to_main_table = SQLExecuteQueryOperator(
        task_id='merge_to_main_table',
        conn_id='redshift-gureum',
        sql=f"""
            INSERT INTO {REDSHIFT_SILVER_SCHEMA}.{TABLE_NAME} (app_id, player_count, result, collected_at)
            SELECT s.app_id, s.player_count, s.result, s.collected_at
            FROM {REDSHIFT_SILVER_SCHEMA}.{STAGING_TABLE_NAME} s
            LEFT JOIN {REDSHIFT_SILVER_SCHEMA}.{TABLE_NAME} t
            ON s.app_id = t.app_id AND s.collected_at = t.collected_at
            WHERE t.app_id IS NULL;
        """,
    )

    for copy_task in copy_tasks:
        copy_task >> merge_to_main_table

    update_ingested_at = SQLExecuteQueryOperator(
        task_id='update_ingested_at',
        conn_id='redshift-gureum',
        sql=f"""
            UPDATE {REDSHIFT_SILVER_SCHEMA}.{TABLE_NAME}
            SET ingested_at = timezone('Asia/Seoul', current_timestamp)
            WHERE ingested_at IS NULL;
        """,
    )
    merge_to_main_table >> update_ingested_at
