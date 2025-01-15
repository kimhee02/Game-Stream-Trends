import sys
import logging
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lit

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("gureum-steam-image-glue-job-logger")

try:
    from pyspark.context import SparkContext
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
except Exception as e:
    logger.error("Error initializing Spark/Glue context or job: %s", str(e))
    raise

try:
    raw_data_path = "s3://gureum-bucket/data/raw/steam/details/2025-01-12/"
    processed_path = "s3://gureum-bucket/data/processed/silver/steam/image/"

    logger.info(f"Processing data from: {raw_data_path}")
    logger.info("Reading raw data from S3...")
    raw_data = spark.read.option("multiline", "true").json(raw_data_path)
    logger.info("Raw data schema:")
    raw_data.printSchema()

    logger.info("Extracting top-level keys...")
    top_level_keys = [field.name for field in raw_data.schema.fields if field.dataType.typeName() == "struct"]
    logger.info("Top-level keys found: %s", top_level_keys)

    data_schema = StructType([
        StructField("header_image", StringType(), True),
        StructField("capsule_image", StringType(), True),
        StructField("capsule_imagev5", StringType(), True),
    ])

    logger.info("Ensuring all app_id columns have a data field...")
    for key in top_level_keys:
        if "data" not in [f.name for f in raw_data.schema[key].dataType.fields]:
            logger.info(f"Adding missing data field for app_id {key}")
            raw_data = raw_data.withColumn(f"`{key}`.data", lit(None).cast(data_schema))

    logger.info("Preparing data for parallel processing...")
    standardized_columns = [
        f"struct('{key}' as app_id, `{key}`.data.header_image as header_image, "
        f"`{key}`.data.capsule_image as capsule_image, "
        f"`{key}`.data.capsule_imagev5 as capsule_imagev5)"
        for key in top_level_keys
    ]

    sql_query = "inline(array(" + ",".join(standardized_columns) + "))"
    logger.info("Generated SQL query: %s", sql_query)

    logger.info("Processing data in parallel using inline...")
    exploded_df = raw_data.selectExpr(sql_query)

    exploded_df = exploded_df \
        .withColumn("ingested_at", lit(None).cast("timestamp")) \
        .withColumn("deleted_at", lit(None).cast("timestamp"))

    logger.info("Transformed data schema:")
    exploded_df.printSchema()
    logger.info("Preview of transformed data:")
    exploded_df.show(5, truncate=False)

    logger.info("Writing transformed data to S3...")
    exploded_df.coalesce(1).write.mode("overwrite").parquet(processed_path)
    logger.info(f"Processed data saved to: {processed_path}")

except Exception as e:
    logger.error("Error processing data: %s", str(e))
    raise

try:
    job.commit()
    logger.info("Glue Job completed successfully.")
except Exception as e:
    logger.error("Error committing Glue Job: %s", str(e))
    raise