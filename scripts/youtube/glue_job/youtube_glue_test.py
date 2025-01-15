import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
import pyspark.sql.functions as F
import datetime

# Glue 작업에 필요한 매개변수 처리
args = getResolvedOptions(sys.argv, ["JOB_NAME", "raw_path", "output_path"])  # Glue Job 이름, 입력 데이터 경로, 출력 데이터 경로
raw_path = args["raw_path"]  # 원본 데이터 경로 (e.g., s3://gureum-bucket/data/raw/youtube/videos/)
output_path = args["output_path"]  # 처리된 데이터를 저장할 S3 경로

# 현재 시각 생성 (collected_at, ingested_at용 - 한국 시간 기준)
current_timestamp = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9))).strftime('%Y-%m-%d %H:%M:%S')

# Glue Job 초기화
sc = SparkContext()  # SparkContext 생성
glueContext = GlueContext(sc)  # GlueContext 생성
spark = glueContext.spark_session  # SparkSession 생성
job = Job(glueContext)  # Glue Job 생성
job.init(args["JOB_NAME"], args)  # Glue Job 초기화

# S3 클라이언트를 사용해 날짜와 시간 폴더 탐색
s3 = boto3.client("s3")
bucket_name = raw_path.replace("s3://", "").split("/")[0]  # S3 버킷 이름 추출
prefix = "/".join(raw_path.replace("s3://", "").split("/")[1:])  # S3 경로의 Prefix

# 날짜 폴더 탐색
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/")
date_folders = [content.get("Prefix") for content in response.get("CommonPrefixes", [])]

# 날짜와 시간 폴더에서 데이터를 처리
for date_folder in date_folders:
    # 시간 폴더 탐색
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=date_folder, Delimiter="/")
    hour_folders = [content.get("Prefix") for content in response.get("CommonPrefixes", [])]

    for hour_folder in hour_folders:
        print(f"Processing: {hour_folder}")

        # Glue에서 해당 경로의 데이터를 로드
        raw_dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [f"s3://{bucket_name}/{hour_folder}"]},
            format="json"
        )

        # DynamicFrame을 Spark DataFrame으로 변환
        raw_df = raw_dynamic_frame.toDF()

        # 필요한 필드만 선택하고 플랫하게 변환
        flattened_df = raw_df.select(
            F.col("id").alias("video_id"),
            F.col("snippet.publishedAt").alias("published_at"),
            F.col("snippet.channelId").alias("channel_id"),
            F.col("snippet.title").alias("title"),
            F.col("snippet.description").alias("description"),
            F.col("snippet.channelTitle").alias("channel_title"),
            F.col("snippet.tags").alias("tags"),
            F.col("statistics.viewCount").cast("long").alias("view_count"),
            F.col("statistics.likeCount").cast("long").alias("like_count"),
            F.col("statistics.commentCount").cast("long").alias("comment_count")
        )

        # 추가 컬럼: collected_at, deleted_at, ingested_at
        flattened_df = flattened_df.withColumn(
            "collected_at", F.lit(current_timestamp)  # 수집 시각 추가 (한국시간 기준)
        ).withColumn(
            "deleted_at", F.lit(None).cast("timestamp")  # 삭제 시간이 없는 경우 null
        ).withColumn(
            "ingested_at", F.lit(current_timestamp)  # Glue 작업 실행 시각 추가 (한국시간 기준)
        )

        # 처리된 데이터를 Parquet 형식으로 S3에 저장
        date_hour = hour_folder.replace(prefix, "").strip("/")  # 날짜/시간 폴더 경로 추출
        output_path_final = f"{output_path}/{date_hour}/"  # 최종 출력 경로 설정
        flattened_df.write.mode("overwrite").parquet(output_path_final)  # Parquet 형식으로 저장

# Glue Job 완료
job.commit()