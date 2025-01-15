from pyspark.sql import SparkSession

# Spark 세션 초기화
spark = SparkSession.builder \
    .appName("Schema Viewer") \
    .config("spark.master", "local") \
    .getOrCreate()

# S3에서 JSON 데이터 읽기
raw_path = "s3://gureum-bucket/data/raw/steam/news/"  # S3 경로 수정
raw_df = spark.read.option("multiline", "true").json(raw_path)

# 스키마 출력
raw_df.printSchema()

# 일부 데이터 출력
raw_df.show(5, truncate=False)