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
args = getResolvedOptions(sys.argv, ["JOB_NAME", "raw_path", "output_path"])
raw_path = args["raw_path"]  # 원본 데이터 경로 (e.g., s3://gureum-bucket/data/raw/youtube/videos/)
output_path = args["output_path"]  # 처리된 데이터를 저장할 S3 경로

# 현재 시각 생성 (collected_at, ingested_at용 - 한국 시간 기준)
current_timestamp = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9))).strftime('%Y-%m-%d %H:%M:%S')

# Glue Job 초기화
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# S3 클라이언트를 사용해 날짜와 시간 폴더 탐색
s3 = boto3.client("s3")
bucket_name = raw_path.replace("s3://", "").split("/")[0]
prefix = "/".join(raw_path.replace("s3://", "").split("/")[1:])

# 날짜 폴더 탐색
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/")
date_folders = [content.get("Prefix") for content in response.get("CommonPrefixes", [])]

# S3에서 JSON 데이터 로드
for date_folder in date_folders:
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=date_folder, Delimiter="/")
    hour_folders = [content.get("Prefix") for content in response.get("CommonPrefixes", [])]

    for hour_folder in hour_folders:
        # S3 객체의 업로드 시간을 가져와 collected_at으로 사용
        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=hour_folder).get("Contents", [])
        if not objects:
            continue  # 폴더에 파일이 없으면 스킵
        latest_object = objects[0]  # 첫 번째 파일 정보 가져오기
        collected_at = latest_object["LastModified"].astimezone(datetime.timezone(datetime.timedelta(hours=9))).strftime('%Y-%m-%d %H:%M:%S')

        print(f"Processing: {hour_folder} | Collected At: {collected_at}")

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
            "collected_at", F.lit(collected_at)  # S3 파일의 업로드 시간
        ).withColumn(
            "deleted_at", F.lit(None).cast("timestamp")  # 삭제 시간이 없는 경우 null
        ).withColumn(
            # Glue 작업 실행 시간 (밀리초 제거)
            "ingested_at", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss").cast("string")
        )

        # 처리된 데이터를 Parquet 형식으로 S3에 저장
        date_hour = hour_folder.replace(prefix, "").strip("/")
        output_path_final = f"{output_path}/{date_hour}/"
        flattened_df.write.mode("overwrite").parquet(output_path_final)

# Glue Job 완료
job.commit()