import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
import pyspark.sql.functions as F
import datetime

# Glue Job 파라미터 설정
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'output_path', 'raw_path'])
output_path = args['output_path']  # 처리된 데이터를 저장할 S3 경로
raw_path = args['raw_path']  # 원본 데이터가 저장된 S3 경로

# 현재 시각 생성 (collected_at, ingested_at용)
current_timestamp = (datetime.datetime.utcnow() + datetime.timedelta(hours=9)).strftime('%Y-%m-%d %H:%M:%S')

# Glue Job 초기화
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3에서 JSON 데이터를 로드하여 DynamicFrame 생성
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [raw_path], "recurse": True},
    format="json"
)

# DynamicFrame을 Spark DataFrame으로 변환
selected_df = datasource.toDF()

# 필요한 필드만 선택하고 중첩된 데이터를 평탄화
flattened_df = selected_df.select(
    F.col("id").alias("id"),
    F.col("name").alias("name"),
    F.col("released").alias("released"),
    F.col("rating").alias("rating"),
    F.col("ratings_count").alias("ratings_count"),
    F.col("reviews_count").alias("reviews_count"),
    F.expr("transform(platforms, x -> x.platform.name)").alias("platforms"),
    F.expr("transform(genres, x -> x.name)").alias("genres"),
    F.expr("transform(tags, x -> x.name)").alias("tags")
)

# 추가 컬럼: collected_at, deleted_at, ingested_at
flattened_df = flattened_df.withColumn(
    "collected_at", F.lit(current_timestamp)  # 현재 시각 (한국 시간)
).withColumn(
    "deleted_at", F.lit(None).cast("string")  # 삭제 시간이 없는 경우 null
).withColumn(
    "ingested_at", F.date_format(F.current_timestamp() + F.expr("INTERVAL 9 HOURS"), "yyyy-MM-dd HH:mm:ss")  # 현재 시각 (한국 시간)
)

# id와 name이 null이 아닌 데이터만 필터링
filtered_df = flattened_df.filter(
    F.col("id").isNotNull() & F.col("name").isNotNull()
)

# 필터링된 DataFrame을 다시 DynamicFrame으로 변환
filtered_dynamic_frame = DynamicFrame.fromDF(
    filtered_df,
    glueContext,
    "filtered_dynamic_frame"
)

# 처리된 데이터를 Parquet 형식으로 S3에 저장
glueContext.write_dynamic_frame.from_options(
    frame=filtered_dynamic_frame,
    connection_type="s3",
    connection_options={"path": f"{output_path}"},
    format="parquet"
)

# Glue Job 완료
job.commit()