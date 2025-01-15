import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
import pyspark.sql.functions as F

# Glue Job 파라미터
# Glue Job 실행 시 전달되는 인자값 중 JOB_NAME과 output_path를 가져옴
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'output_path'])
output_path = args['output_path']  # 처리된 데이터를 저장할 S3 경로
raw_path = "s3://gureum-bucket/data/raw/RAWG/games_json"  # 원본 JSON 데이터가 저장된 S3 경로

# Glue Job 초기화
# SparkContext 및 GlueContext를 초기화하여 Spark 작업 환경 설정
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# DynamicFrame 생성 (S3에서 데이터 로드)
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [raw_path], "recurse": True},  # 하위 폴더 데이터 포함
    format="json"  # JSON 형식의 데이터를 처리
)

# DynamicFrame을 DataFrame으로 변환
selected_df = datasource.toDF()

# 중첩 데이터 평탄화 및 필요한 필드 선택
flattened_df = selected_df.select(
    F.col("id").alias("id"),  # 최상위 id
    F.col("name").alias("name"),  # 게임 이름
    F.col("released").alias("released"),  # 출시 날짜
    F.col("rating").alias("rating"),  # 평점
    F.col("ratings_count").alias("ratings_count"),  # 평점 수
    F.col("reviews_count").alias("reviews_count"),  # 리뷰 수
    F.expr("transform(platforms, x -> x.platform.name)").alias("platforms"),  # 플랫폼 이름
    F.expr("transform(genres, x -> x.name)").alias("genres"),  # 장르 이름
    F.expr("transform(tags, x -> x.name)").alias("tags")  # 태그 이름
)

# 평탄화된 DataFrame을 다시 DynamicFrame으로 변환
# 처리된 DataFrame을 DynamicFrame으로 변환하여 Glue에서 사용할 수 있도록 설정
flattened_dynamic_frame = DynamicFrame.fromDF(
    flattened_df,
    glueContext,
    "flattened_dynamic_frame"
)

# 평탄화된 데이터를 Parquet 형식으로 S3에 저장
glueContext.write_dynamic_frame.from_options(
    frame=flattened_dynamic_frame,
    connection_type="s3",
    connection_options={"path": f"{output_path}"},  # S3 저장 경로
    format="parquet"  # 저장 형식: Parquet
)

# Glue Job 완료
job.commit()