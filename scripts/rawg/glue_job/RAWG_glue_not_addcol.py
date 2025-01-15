import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
import pyspark.sql.functions as F

# Glue Job 파라미터 설정
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'output_path'])  # Glue Job 실행 시 전달된 파라미터
output_path = args['output_path']  # 처리된 데이터를 저장할 S3 경로
raw_path = "s3://gureum-bucket/data/raw/RAWG/games_json"  # 원본 데이터가 저장된 S3 경로

# Glue Job 초기화
sc = SparkContext()  # SparkContext 생성
glueContext = GlueContext(sc)  # GlueContext 생성
spark = glueContext.spark_session  # Spark 세션 생성
job = Job(glueContext)  # Glue Job 객체 생성
job.init(args['JOB_NAME'], args)  # Glue Job 초기화

# S3에서 JSON 데이터를 로드하여 DynamicFrame 생성
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",  # S3에서 데이터를 로드
    connection_options={"paths": [raw_path], "recurse": True},  # 지정된 경로와 하위 디렉토리 포함
    format="json"  # JSON 형식 데이터
)

# DynamicFrame을 Spark DataFrame으로 변환
selected_df = datasource.toDF()

# 필요한 필드만 선택하고 중첩된 데이터를 평탄화
flattened_df = selected_df.select(
    F.col("id").alias("id"),  # 최상위 id
    F.col("name").alias("name"),  # 게임 이름
    F.col("released").alias("released"),  # 출시 날짜
    F.col("rating").alias("rating"),  # 평점
    F.col("ratings_count").alias("ratings_count"),  # 평점 수
    F.col("reviews_count").alias("reviews_count"),  # 리뷰 수
    F.expr("transform(platforms, x -> x.platform.name)").alias("platforms"),  # platforms의 name 평탄화
    F.expr("transform(genres, x -> x.name)").alias("genres"),  # genres의 name 평탄화
    F.expr("transform(tags, x -> x.name)").alias("tags")  # tags의 name 평탄화
)

# id와 name이 null이 아닌 데이터만 필터링
filtered_df = flattened_df.filter(
    F.col("id").isNotNull() & F.col("name").isNotNull()  # id와 name이 모두 null이 아닌 레코드를 유지
)

# 필터링된 DataFrame을 다시 DynamicFrame으로 변환
filtered_dynamic_frame = DynamicFrame.fromDF(
    filtered_df,  # 필터링된 DataFrame
    glueContext,  # GlueContext
    "filtered_dynamic_frame"  # DynamicFrame 이름
)

# 처리된 데이터를 Parquet 형식으로 S3에 저장
glueContext.write_dynamic_frame.from_options(
    frame=filtered_dynamic_frame,  # 저장할 DynamicFrame
    connection_type="s3",  # S3 연결 유형
    connection_options={"path": f"{output_path}"},  # 저장 경로
    format="parquet"  # Parquet 형식으로 저장
)

# Glue Job 완료
job.commit() 