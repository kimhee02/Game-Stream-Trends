import sys
import pytz
import boto3
import logging
from awsglue.utils import getResolvedOptions
from datetime import datetime, timedelta

from table_metadata import TABLES

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("gureum-s3-to-redshift-glue-job-logger")

args = getResolvedOptions(
    sys.argv,
    [
        "REDSHIFT_SILVER_SCHEMA",
        "REDSHIFT_IAM_ROLE",
        "S3_BUCKET_NAME",
        "S3_SILVER_BASE_PATH",
        "REDSHIFT_SECRET_ARN",
    ],
)

REDSHIFT_SILVER_SCHEMA = args["REDSHIFT_SILVER_SCHEMA"]
REDSHIFT_IAM_ROLE = args["REDSHIFT_IAM_ROLE"]
S3_BUCKET_NAME = args["S3_BUCKET_NAME"]
S3_SILVER_BASE_PATH = args["S3_SILVER_BASE_PATH"]
REDSHIFT_SECRET_ARN = args["REDSHIFT_SECRET_ARN"]

print(f"REDSHIFT_SILVER_SCHEMA: {REDSHIFT_SILVER_SCHEMA}")
print(f"REDSHIFT_IAM_ROLE: {REDSHIFT_IAM_ROLE}")
print(f"S3_BUCKET_NAME: {S3_BUCKET_NAME}")
print(f"S3_SILVER_BASE_PATH: {S3_SILVER_BASE_PATH}")
print(f"REDSHIFT_SECRET_ARN: {REDSHIFT_SECRET_ARN}")


s3_client = boto3.client("s3")
redshift_client = boto3.client("redshift-data")

# query_id = "baf571e7-fc5e-4fec-9e90-9f8778b83630"
# result = redshift_client.describe_statement(Id=query_id)
# print("result:")
# print(result)

def generate_valid_s3_paths(source, category, interval):
    logger.info(f"Generating valid S3 paths for {category}...")
    valid_paths = []

    kst_now = datetime.now(pytz.timezone("Asia/Seoul"))
    target_date = kst_now - timedelta(days=1)

    try:
        if interval == "4-hourly":
            for hour in range(24):
                s3_path = f"{S3_SILVER_BASE_PATH}/{source}/{category}/{target_date.strftime('%Y-%m-%d')}/{hour:02d}/"
                objects = list_s3_objects(S3_BUCKET_NAME, s3_path)
                parquet_files = [obj for obj in objects if obj.endswith(".parquet")]
                if parquet_files:
                    valid_paths.append(s3_path)
        elif interval == "daily":
            s3_path = f"{S3_SILVER_BASE_PATH}/{source}/{category}/{target_date.strftime('%Y-%m-%d')}/"
            objects = list_s3_objects(S3_BUCKET_NAME, s3_path)
            parquet_files = [obj for obj in objects if obj.endswith(".parquet")]
            if parquet_files:
                valid_paths.append(s3_path)
    except Exception as e:
        logger.error(f"Error while generating valid S3 paths: {e}")
        raise

    logger.info(f"Valid S3 paths: {valid_paths}")
    return valid_paths
    
def list_s3_objects(bucket_name, prefix):
    logger.debug(f"Listing S3 objects in bucket: {bucket_name}, prefix: {prefix}")
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        keys = [obj["Key"] for obj in response.get("Contents", [])] if "Contents" in response else []
        logger.debug(f"Found S3 objects: {keys}")
        return keys
    except Exception as e:
        logger.error(f"Error while listing S3 objects: {e}")
        raise

def execute_redshift_query(query):
    logger.info(f"Executing Redshift query: {query}")
    try:
        response = redshift_client.execute_statement(
            ClusterIdentifier="gureum-redshift-cluster-1",
            Database="dev",
            SecretArn=REDSHIFT_SECRET_ARN,
            Sql=query
        )
        query_id = response['Id']
        logger.info(f"Query initiated with ID: {query_id}")
    except Exception as e:
        logger.error(f"Error while executing Redshift query: {e}")
        raise

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
        
        try:
            s3_paths = generate_valid_s3_paths(source, category, interval)

            drop_staging_table_query = f"DROP TABLE IF EXISTS {REDSHIFT_SILVER_SCHEMA}.{table_name}_staging;"
            execute_redshift_query(drop_staging_table_query)

            execute_redshift_query(staging_table_schema)

            for s3_path in s3_paths:
                copy_query = f"""
                    COPY {REDSHIFT_SILVER_SCHEMA}.{table_name}_staging ({', '.join(columns)})
                    FROM 's3://{S3_BUCKET_NAME}/{s3_path}'
                    IAM_ROLE '{REDSHIFT_IAM_ROLE}'
                    FORMAT AS PARQUET;
                """
                execute_redshift_query(copy_query)

            merge_query = f"""
                INSERT INTO {REDSHIFT_SILVER_SCHEMA}.{table_name} ({', '.join(columns)})
                SELECT {', '.join([f's.{col}' for col in columns])}
                FROM {REDSHIFT_SILVER_SCHEMA}.{table_name}_staging s
                LEFT JOIN {REDSHIFT_SILVER_SCHEMA}.{table_name} t
                ON {join_condition}
                WHERE t.{unique_val} IS NULL;
            """
            execute_redshift_query(merge_query)

            update_query = f"""
                UPDATE {REDSHIFT_SILVER_SCHEMA}.{table_name}
                SET ingested_at = timezone('Asia/Seoul', current_timestamp)
                WHERE ingested_at IS NULL;
            """
            execute_redshift_query(update_query)
        except Exception as e:
            logger.error(f"Error processing table {table_name} in source {source}: {e}")
            continue
logger.info("Glue Job completed successfully.")