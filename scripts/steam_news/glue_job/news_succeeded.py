import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import pyspark.sql.functions as F
from datetime import datetime
from botocore.exceptions import ClientError

# Glue 작업에 필요한 매개변수 처리
args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_path", "output_path"])
input_path = args["input_path"]  # 원본 데이터가 저장된 S3 경로 (e.g., s3://gureum-bucket/data/raw/steam/news/)
output_path = args["output_path"]  # 처리된 데이터를 저장할 S3 경로 (e.g., s3://gureum-bucket/data/processed/silver/steam/news/)

# Glue Job 초기화
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# S3 클라이언트 생성
s3 = boto3.client("s3")

# S3 경로에서 모든 yyyy-mm-dd 형식의 폴더 탐색
def get_folders(bucket, prefix):
    try:
        result = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
        if "CommonPrefixes" in result:
            return [folder["Prefix"] for folder in result["CommonPrefixes"]]
        else:
            return []
    except ClientError as e:
        print(f"Error accessing S3 bucket: {e}")
        return []

# S3 경로에서 파일 읽기
def process_files(bucket, folder):
    try:
        result = s3.list_objects_v2(Bucket=bucket, Prefix=folder)
        if "Contents" in result:
            return [file["Key"] for file in result["Contents"] if file["Key"].endswith(".json")]
        else:
            return []
    except ClientError as e:
        print(f"Error accessing files in S3 folder: {e}")
        return []

# S3 bucket 및 prefix 추출
bucket_name = input_path.replace("s3://", "").split("/")[0]
prefix = "/".join(input_path.replace("s3://", "").split("/")[1:])

# yyyy-mm-dd 폴더 목록 가져오기
folders = get_folders(bucket_name, prefix)

for folder in folders:
    # 각 폴더에서 JSON 파일 목록 가져오기
    json_files = process_files(bucket_name, folder)

    for json_file in json_files:
        print(f"Processing file: {json_file}")

        # JSON 파일 읽기
        raw_df = spark.read.option("multiline", "true").json(f"s3://{bucket_name}/{json_file}")

        # 필요한 필드 선택 및 처리
        processed_df = raw_df.select(
            F.col("app_id").alias("app_id"),
            F.col("gid").alias("gid"),
            F.col("title").alias("title"),
            F.col("author").alias("author"),
            F.col("contents").alias("contents"),
            F.col("date").cast("long").alias("date"),
            F.col("tags").alias("tags")
        )

        # 처리된 데이터를 Parquet 형식으로 저장
        output_folder = f"{output_path}/{folder.split('/')[-2]}"
        processed_df.write.mode("overwrite").parquet(f"s3://{bucket_name}/{output_folder}")

# Glue Job 완료
job.commit()
