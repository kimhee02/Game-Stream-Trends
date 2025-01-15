import boto3
import sys
import pytz
import logging
from datetime import datetime, timedelta
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from botocore.exceptions import ClientError
from pyspark.sql import Row
from pyspark.sql.functions import to_timestamp, lit
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import col, lit, regexp_extract, input_file_name, to_timestamp, when

KST = pytz.timezone('Asia/Seoul')

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("gureum-steam-players-in-use-glue-job-logger")

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
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "table_name", "base_output_path", "base_input_path"])
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
    table_name = args["table_name"]
    base_output_path = args["base_output_path"]
    base_input_path = args["base_input_path"]

    now = datetime.now(KST)
    start_time = now - timedelta(days=1)
    end_time = now
    
    file_save_count = 0
    
    timestamp_pattern = r".*_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})\.json"
    current_time = start_time
    while current_time <= end_time:
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
            logger.info("Reading raw data from S3...")
            raw_data = spark.read.option("multiline", "true").json(raw_data_path)
            logger.info("Raw data schema:")
            raw_data.printSchema()

            logger.info("Extracting top-level keys...")
            top_level_keys = [field.name for field in raw_data.schema.fields if field.dataType.typeName() == "struct"]
            logger.info("Top-level keys found: %s", top_level_keys)
            
            logger.info("Extracting file path and collected_at timestamp...")
            raw_data = raw_data.withColumn("file_path", input_file_name())
            raw_data = raw_data.withColumn(
                "collected_at_raw", 
                regexp_extract(col("file_path"), timestamp_pattern, 1)
            )
            
            last_modified = get_latest_last_modified(raw_data_path)
            if last_modified:
                last_modified_kst = last_modified.astimezone(KST)
                last_modified_str = last_modified_kst.strftime("%Y-%m-%d_%H-%M-%S")
            else:
                last_modified_str = None

            default_timestamp = (
                last_modified_str if last_modified_str else f"{formatted_date}_{formatted_hour}-00-00"
            )

            raw_data = raw_data.withColumn(
                "collected_at",
                to_timestamp(
                    when(raw_data["collected_at_raw"] != "", raw_data["collected_at_raw"])
                    .otherwise(lit(default_timestamp)),
                    "yyyy-MM-dd_HH-mm-ss"
                )
            )

            response_schema = StructType([
                StructField("player_count", IntegerType(), True),
                StructField("result", IntegerType(), True),
            ])

            logger.info("Ensuring all app_id columns have a response field...")
            for key in top_level_keys:
                if "response" not in [f.name for f in raw_data.schema[key].dataType.fields]:
                    logger.info(f"Adding missing response field for app_id {key}")
                    raw_data = raw_data.withColumn(f"`{key}`.response", lit(None).cast(response_schema))

            logger.info("Preparing data for parallel processing...")
            standardized_columns = [
                f"struct('{key}' as app_id, `{key}`.response.player_count as player_count, "
                f"`{key}`.response.result as result, "
                f"collected_at as collected_at)"
                for key in top_level_keys
            ]

            logger.info("Processing data in parallel using inline...")
            exploded_df = raw_data.selectExpr(
                "inline(array(" + ",".join(standardized_columns) + "))"
            )

            logger.info("Transformed data schema:")
            exploded_df.printSchema()
            logger.info("Preview of transformed data:")
            exploded_df.show(5, truncate=False)

            logger.info("Writing transformed data to S3...")
            exploded_df.coalesce(1).write.mode("overwrite").parquet(processed_path)
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