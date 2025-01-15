import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
import pyspark.sql.functions as F
import re

# Glue 작업에 필요한 매개변수 처리
args = getResolvedOptions(sys.argv, ["JOB_NAME", "raw_path", "output_path"])
raw_path = args["raw_path"]  # 원본 데이터 경로 (e.g., s3://gureum-bucket/data/raw/steam/news/)
output_path = args["output_path"]  # 처리된 데이터를 저장할 S3 경로 (e.g., s3://gureum-bucket/data/processed/silver/steam/news/)

# Glue Job 초기화
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# S3 클라이언트 생성
s3 = boto3.client("s3")

# 날짜 형식의 폴더 탐색 (YYYY-MM-DD 형식만 필터링)
def get_date_folders(bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    if "CommonPrefixes" in response:
        # 날짜 형식(YYYY-MM-DD)에 맞는 폴더만 필터링
        return [
            content["Prefix"]
            for content in response["CommonPrefixes"]
            if re.match(r".*/\d{4}-\d{2}-\d{2}/$", content["Prefix"])
        ]
    return []

# S3 bucket 및 prefix 추출
bucket_name = raw_path.replace("s3://", "").split("/")[0]  # 버킷 이름 추출
prefix = "/".join(raw_path.replace("s3://", "").split("/")[1:])  # Prefix 추출

# 날짜 형식 폴더 가져오기
date_folders = get_date_folders(bucket_name, prefix)

# JSON 파일 처리
for date_folder in date_folders:
    print(f"Processing folder: {date_folder}")
    try:
        # S3에서 JSON 데이터 읽기
        raw_dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [f"s3://{bucket_name}/{date_folder}"]},
            format="json"
        )

        # DynamicFrame을 DataFrame으로 변환
        raw_df = raw_dynamic_frame.toDF()

        # 스키마 확인 및 데이터 미리보기 (디버깅용)
        raw_df.printSchema()
        raw_df.show(5, truncate=False)

        # 최상위 숫자 키를 동적으로 처리하기 위해 selectExpr 사용
        flattened_df = raw_df.selectExpr("stack(1, *.*) as (key, app_data)")  # 최상위 키와 하위 데이터를 분리

        # appnews 데이터 추출
        appnews_df = flattened_df.select(
            F.col("app_data.appnews.appid").alias("app_id"),  # app_id 추출
            F.explode(F.col("app_data.appnews.newsitems")).alias("news_item")  # newsitems 배열을 플랫하게 변환
        )

        # newsitems 내부 필드 추출 (null 처리 추가)
        final_df = appnews_df.select(
            F.col("app_id"),
            F.when(F.col("news_item.gid").isNotNull(), F.col("news_item.gid")).alias("gid"),  # gid에 값이 없으면 null
            F.when(F.col("news_item.title").isNotNull(), F.col("news_item.title")).alias("title"),  # title에 값이 없으면 null
            F.when(F.col("news_item.author").isNotNull(), F.col("news_item.author")).alias("author"),  # author에 값이 없으면 null
            F.when(F.col("news_item.contents").isNotNull(), F.col("news_item.contents")).alias("contents"),  # contents 처리
            F.when(F.col("news_item.date").isNotNull(), F.col("news_item.date").cast("long")).alias("date"),  # date 처리
            F.when(F.col("news_item.tags").isNotNull(), F.col("news_item.tags")).alias("tags")  # tags 처리
        )

        # Output 경로 설정
        date_folder_name = date_folder.strip("/").split("/")[-1]  # 날짜 폴더 이름 추출
        output_path_final = f"{output_path}/{date_folder_name}/"

        # 처리된 데이터를 Parquet 형식으로 저장
        final_df.write.mode("overwrite").parquet(output_path_final)

    except Exception as e:
        # 에러 발생 시 로그 출력
        print(f"Error processing folder {date_folder}: {str(e)}")
        continue

# Glue Job 완료
job.commit()
