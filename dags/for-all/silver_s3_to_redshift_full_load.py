from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from datetime import datetime, timedelta
import logging

import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from table_metadata import TABLES

START_DATE = datetime(2024, 12, 30)
END_DATE = datetime(2025, 1, 11)

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
    current_date = START_DATE

    while current_date <= END_DATE:
        if interval == '4-hourly':
            for hour in range(24):
                s3_path = f"{S3_SILVER_BASE_PATH}/{source}/{category}/{current_date.strftime('%Y-%m-%d')}/{hour:02d}/"
                objects = s3_hook.list_keys(bucket_name=S3_BUCKET_NAME, prefix=s3_path)
                parquet_files = [obj for obj in objects if obj.endswith('.parquet')] if objects else []
                if parquet_files:
                    valid_paths.append(s3_path)
        elif interval == 'daily':
            s3_path = f"{S3_SILVER_BASE_PATH}/{source}/{category}/{current_date.strftime('%Y-%m-%d')}/"
            objects = s3_hook.list_keys(bucket_name=S3_BUCKET_NAME, prefix=s3_path)
            parquet_files = [obj for obj in objects if obj.endswith('.parquet')] if objects else []
            if parquet_files:
                valid_paths.append(s3_path)
        
        current_date += timedelta(days=1)

    logging.info(f"Valid S3 paths for {category}: {valid_paths}")
    return valid_paths

with DAG(
    dag_id='silver_s3_to_redshift_full_load',
    default_args=default_args,
    description='START_DATE ~ END_DATE 사이의 S3에 적재된 Silver Layer를 Redshift로 COPY',
    schedule_interval=None,
    catchup=False,
    tags=['steam', 'twitch', 'youtube', 'silver', 'trigger-only'],
) as dag:

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
            with TaskGroup(group_id=f"{table_name}_copy_tasks") as copy_task_group:
                for s3_path in s3_paths:
                    copy_to_staging = SQLExecuteQueryOperator(
                        task_id=f'copy_to_{table_name}_staging_{s3_path.split('/')[-3]}_{s3_path.split('/')[-2]}_{s3_path.split('/')[-1]}',
                        conn_id='redshift-gureum',
                        sql=f"""
                            COPY {REDSHIFT_SILVER_SCHEMA}.{table_name}_staging ({', '.join(columns)})
                            FROM 's3://{S3_BUCKET_NAME}/{s3_path}'
                            IAM_ROLE '{REDSHIFT_IAM_ROLE}'
                            FORMAT AS PARQUET;
                        """,
                    )
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

            drop_staging_table >> create_staging_table >> copy_task_group
            copy_task_group >> merge_to_main_table >> update_ingested_at