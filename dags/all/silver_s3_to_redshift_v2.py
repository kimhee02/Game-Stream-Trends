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

with DAG(
    dag_id='silver_s3_to_redshift_v2',
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
            timeout=1800,
            poke_interval=300,
        )
        sensors.append(sensor)

    trigger_glue_job_s3_to_redshift = GlueJobOperator(
        task_id="trigger_glue_job_s3_to_redshift",
        job_name="gureum-s3-to-redshift",
        region_name="ap-northeast-2",
        aws_conn_id="aws_gureum",
        wait_for_completion=True,
        dag=dag,
    )

sensors >> trigger_glue_job_s3_to_redshift

