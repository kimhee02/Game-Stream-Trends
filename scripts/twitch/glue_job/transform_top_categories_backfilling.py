import sys
import logging
import boto3
import pytz
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import input_file_name, regexp_extract, to_timestamp, when, lit

KST = pytz.timezone('Asia/Seoul')

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("gureum-twitch-top-categories-backfilling-glue-job")

def s3_path_exists(s3_path):
    s3 = boto3.client("s3")
    bucket_name = "gureum-bucket"
    prefix = s3_path.replace(f"s3://{bucket_name}/", "")
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        return "Contents" in response
    except ClientError as e:
        logger.error("Error checking S3 path: %s", str(e))
        return False
    except Exception as e:
        logger.error("Unexpected error in s3_path_exists: %s", str(e))
        return False
    
def get_latest_last_modified(s3_path):
    s3 = boto3.client("s3")
    bucket_name = "gureum-bucket"
    prefix = s3_path.replace(f"s3://{bucket_name}/", "")
    
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if "Contents" in response and len(response["Contents"]) > 0:
            latest_modified = max(obj["LastModified"] for obj in response["Contents"])
            return latest_modified
        else:
            logger.info(f"No files found at path: {s3_path}")
            return None
    except ClientError as e:
        logger.error("Error retrieving S3 Last Modified: %s", str(e))
        return None
    except Exception as e:
        logger.error("Unexpected error in get_latest_last_modified: %s", str(e))
        return None

try:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "start_date", "end_date", "table_name", "base_output_path", "base_input_path"])
except Exception as e:
    logger.error("Error parsing job arguments: %s", str(e))
    raise
try:
    from pyspark.context import SparkContext
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)
except Exception as e:
    logger.error("Error initializing Spark/Glue context or job: %s", str(e))
    raise

try:
    start_date = datetime.strptime(args["start_date"], "%Y-%m-%d")
    end_date = datetime.strptime(args["end_date"], "%Y-%m-%d") + timedelta(days=1)
    table_name = args["table_name"]
    base_input_path = args["base_input_path"]
    base_output_path = args["base_output_path"]

    json_schema = StructType([
        StructField("data", ArrayType(StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("box_art_url", StringType(), True),
            StructField("igdb_id", StringType(), True)
        ])), True),
        StructField("pagination", StructType([
            StructField("cursor", StringType(), True)
        ]), True)
    ])

    file_save_count = 0

    timestamp_pattern = r".*_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})\.json"
    current_time = start_date
    while current_time <= end_date:
        try:
            formatted_date = current_time.strftime("%Y-%m-%d")
            formatted_hour = current_time.strftime("%H")
            raw_data_path = f"{base_input_path}/{table_name}/{formatted_date}/{formatted_hour}/"
            processed_path = f"{base_output_path}/{table_name}/{formatted_date}/{formatted_hour}/"

            if not s3_path_exists(raw_data_path):
                logger.info("Raw data path does not exist, skipping: %s", raw_data_path)
                current_time += timedelta(hours=1)
                continue

            if s3_path_exists(processed_path):
                logger.info("Skipping already processed path: %s", processed_path)
                current_time += timedelta(hours=1)
                continue

            logger.info(f"Processing data from: {raw_data_path}")
            raw_df = spark.read.option("multiline", "true").schema(json_schema).json(raw_data_path)
            
            raw_df = raw_df.withColumn("file_path", input_file_name())
            
            raw_df = raw_df.withColumn(
                "collected_at_raw",
                regexp_extract("file_path", timestamp_pattern, 1)
            )
            print("1st raw_df")
            raw_df.show(truncate=False)
            
            last_modified = get_latest_last_modified(raw_data_path)
            if last_modified:
                last_modified_kst = last_modified.astimezone(KST)
                last_modified_str = last_modified_kst.strftime("%Y-%m-%d_%H-%M-%S")
            else:
                last_modified_str = None
            
            default_timestamp = (
                last_modified_str if last_modified_str else f"{formatted_date}_{formatted_hour}-00-00"
            )
            
            raw_df = raw_df.withColumn(
                "collected_at",
                to_timestamp(
                    when(raw_df["collected_at_raw"] != "", raw_df["collected_at_raw"])
                    .otherwise(lit(default_timestamp)),
                    "yyyy-MM-dd_HH-mm-ss"
                )
            )
            print("2nd raw_df")
            raw_df.show(truncate=False)
            
            ranked_df = raw_df.selectExpr("posexplode(data) as (pos, element)", "collected_at")
            
            ranked_df = ranked_df.withColumn("rank", ranked_df["pos"] + 1)
            
            processed_df = ranked_df.select(
                ranked_df["element.id"].alias("id"),
                ranked_df["element.name"].alias("name"),
                ranked_df["element.igdb_id"].alias("igdb_id"),
                ranked_df["rank"],
                ranked_df["collected_at"]
            )
            
            processed_df.show(truncate=False)
            
            processed_df.coalesce(1).write.mode("overwrite").parquet(processed_path)
            logger.info(f"Processed data saved to: {processed_path}")
            
            file_save_count += 1

        except Exception as e:
            logger.error("Error processing data for date %s and hour %s: %s", formatted_date, formatted_hour, str(e))

        current_time += timedelta(hours=1)
        
    logger.info(f"Total files saved: {file_save_count}")

    try:
        job.commit()
        logger.info("Glue Job completed successfully.")
    except Exception as e:
        logger.error("Error committing Glue Job: %s", str(e))
        raise

except Exception as e:
    logger.error("Unexpected error in Glue Job: %s", str(e))
    raise
