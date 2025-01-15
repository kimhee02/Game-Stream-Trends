import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.functions as F
import time
import datetime
from pyspark.sql.functions import date_format


# Glue 작업에 필요한 매개변수 처리
args = getResolvedOptions(sys.argv, ["JOB_NAME", "raw_path", "output_path"]) # Glue 작업 이름, 입력 데이터 경로, 출력 데이터 경로
raw_path = args["raw_path"]  # 원본 데이터가 저장된 S3 경로
output_path = args['output_path']  # 처리된 데이터를 저장할 S3 경로

# 현재 시각 생성 (collected_at)
current_timestamp = datetime.datetime.fromtimestamp(int(time.time())).strftime('%Y-%m-%d %H:%M:%S')

# Glue Job 초기화
sc = SparkContext()  # SparkContext 생성
glueContext = GlueContext(sc)  # GlueContext 생성
spark = glueContext.spark_session  # SparkSession 생성
job = Job(glueContext)  # Glue Job 생성
job.init(args["JOB_NAME"], args)  # Glue Job 초기화

# S3에서 JSON 데이터 로드
raw_dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",  # 데이터 소스 타입(S3)
    connection_options={"paths": [raw_path], "recurse": True},  # 데이터 경로 및 하위 폴더 탐색 설정
    format="json"  # 데이터 형식(JSON)
)

# DynamicFrame을 Spark DataFrame으로 변환
raw_df = raw_dynamic_frame.toDF()

# 필요한 필드만 선택하고 플랫하게 변환
flattened_df = raw_df.select(
    F.col("id").alias("video_id"),  # 동영상 ID
    F.col("snippet.publishedAt").alias("published_at"),  # 게시 날짜
    F.col("snippet.channelId").alias("channel_id"),  # 채널 ID
    F.col("snippet.title").alias("title"),  # 동영상 제목
    F.col("snippet.description").alias("description"),  # 동영상 설명
    F.col("snippet.channelTitle").alias("channel_title"),  # 채널 이름
    F.col("snippet.tags").alias("tags"),  # 태그
    F.col("statistics.viewCount").cast("long").alias("view_count"),  # 조회수 (정수형 변환)
    F.col("statistics.likeCount").cast("long").alias("like_count"),  # 좋아요 수 (정수형 변환)
    F.col("statistics.commentCount").cast("long").alias("comment_count")  # 댓글 수 (정수형 변환)
)

# 추가 컬럼: collected_at, deleted_at, ingested_at
flattened_df = flattened_df.withColumn(
    "collected_at", 
    date_format(F.lit(current_timestamp), "yyyy-MM-dd HH:mm:ss.SSS") 
) # 밀리초 형식 수정
flattened_df = flattened_df.withColumn("deleted_at", F.lit(None).cast("timestamp"))  # 삭제 시간이 없는 경우 null
flattened_df = flattened_df.withColumn(
    "ingested_at", 
    date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSS")
) # 밀리초 형식 수정

# 처리된 데이터를 Parquet 형식으로 S3에 저장
output_path_final = f"{output_path}"  # 최종 출력 경로 설정
flattened_df.write.mode("overwrite").parquet(output_path_final)  # Parquet 형식으로 덮어쓰기 저장

# Glue Job 완료
job.commit()