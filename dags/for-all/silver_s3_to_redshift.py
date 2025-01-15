from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
from pytz import timezone

import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from table_metadata import TABLES

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

def generate_valid_s3_paths(source, category, interval):
    logging.info(f"Generating valid S3 paths for {category}...")
    s3_hook = S3Hook(aws_conn_id='aws_gureum')
    valid_paths = []

    kst_now = datetime.now(timezone('Asia/Seoul'))
    target_date = kst_now - timedelta(days=1)

    if interval == '4-hourly':
        for hour in range(24):
            s3_path = f"{S3_SILVER_BASE_PATH}/{source}/{category}/{target_date.strftime('%Y-%m-%d')}/{hour:02d}/"
            objects = s3_hook.list_keys(bucket_name=S3_BUCKET_NAME, prefix=s3_path)
            parquet_files = [obj for obj in objects if obj.endswith('.parquet')] if objects else []
            if parquet_files:
                valid_paths.append(s3_path)
    elif interval == 'daily':
        s3_path = f"{S3_SILVER_BASE_PATH}/{source}/{category}/{target_date.strftime('%Y-%m-%d')}/"
        objects = s3_hook.list_keys(bucket_name=S3_BUCKET_NAME, prefix=s3_path)
        parquet_files = [obj for obj in objects if obj.endswith('.parquet')] if objects else []
        if parquet_files:
            valid_paths.append(s3_path)

    logging.info(f"Valid S3 paths for {category}: {valid_paths}")
    return valid_paths

with DAG(
    dag_id='silver_s3_to_redshift',
    default_args=default_args,
    description='전날 S3에 적재된 Silver Layer를 Redshift로 COPY',
    schedule_interval="0 15 * * *",
    catchup=False,
    tags=['steam', 'twitch', 'youtube', 'silver', 'daily'],
) as dag:

    sensor_configs = [
        {'task_id': 'wait_for_steam_silver_daily', 'external_dag_id': 'steam_silver_daily'},
        {'task_id': 'wait_for_steam_silver_4hourly', 'external_dag_id': 'steam_silver_4hourly'},
        {'task_id': 'wait_for_twitch_silver', 'external_dag_id': 'twitch_silver'},
        {'task_id': 'wait_for_youtube_silver', 'external_dag_id': 'youtube_silver_v2'},
    ]

    sensors = []
    for config in sensor_configs:
        sensor = ExternalTaskSensor(
            task_id=config['task_id'],
            external_dag_id=config['external_dag_id'],
            external_task_id=None,
            mode='reschedule',
            timeout=3600,
            poke_interval=300,
        )
        sensors.append(sensor)

    for source, tables in TABLES.items():
        print(f"Processing source: {source}")
        for table in tables:
            category = table['category']
            interval = table['interval']
            table_name = table['table_name']
            staging_table_schema = table['staging_schema']
            columns = table['columns']
            join_condition = table['join_condition']
            unique_val = table['unique_val']
            
            s3_paths = generate_valid_s3_paths(source, category, interval)

            drop_staging_table = SQLExecuteQueryOperator(
                task_id=f'drop_staging_table_{table_name}',
                conn_id='redshift-gureum',
                sql=f"DROP TABLE IF EXISTS {REDSHIFT_SILVER_SCHEMA}.{table_name}_staging;",
            )

            create_staging_table = SQLExecuteQueryOperator(
                task_id=f'create_staging_table_{table_name}',
                conn_id='redshift-gureum',
                sql=staging_table_schema,
            )

            copy_tasks = [] 
            for s3_path in s3_paths:
                copy_to_staging = SQLExecuteQueryOperator(
                    task_id=f'copy_to_{table_name}_staging_{s3_path.split("/")[-2]}_{s3_path.split("/")[-1]}',
                    conn_id='redshift-gureum',
                    sql=f"""
                        COPY {REDSHIFT_SILVER_SCHEMA}.{table_name}_staging ({', '.join(columns)})
                        FROM 's3://{S3_BUCKET_NAME}/{s3_path}'
                        CREDENTIALS 'aws_iam_role={REDSHIFT_IAM_ROLE}'
                        FORMAT AS PARQUET;
                    """,
                )
                sensors >> drop_staging_table >> create_staging_table >> copy_to_staging
                copy_tasks.append(copy_to_staging)

            merge_to_main_table = SQLExecuteQueryOperator(
                task_id=f'merge_to_main_table_{table_name}',
                conn_id='redshift-gureum',
                sql=f"""
                    INSERT INTO {REDSHIFT_SILVER_SCHEMA}.{table_name} ({', '.join(columns)})
                    SELECT {', '.join([f's.{col}' for col in columns])}
                    FROM {REDSHIFT_SILVER_SCHEMA}.{table_name}_staging s
                    LEFT JOIN {REDSHIFT_SILVER_SCHEMA}.{table_name} t
                    ON {join_condition}
                    WHERE t.{unique_val} IS NULL;
                """,
            )

            update_ingested_at = SQLExecuteQueryOperator(
                task_id=f'update_ingested_at_{table_name}',
                conn_id='redshift-gureum',
                sql=f"""
                    UPDATE {REDSHIFT_SILVER_SCHEMA}.{table_name}
                    SET ingested_at = timezone('Asia/Seoul', current_timestamp)
                    WHERE ingested_at IS NULL;
                """,
            )
            for copy_task in copy_tasks:
                copy_task >> merge_to_main_table
            merge_to_main_table >> update_ingested_at