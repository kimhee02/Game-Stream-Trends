```
s3://{bucket-name}/
│
├── data/
│   ├── raw/
│   │   ├── {data_source}/  # steam, twitch, youtube
│   │   │   ├── {data_type}/  # app_id, details, news, ...
│   │   │   │   ├── {YYYY-MM-DD}/  # e.g., 2025-01-01
│   │   │   │   │   ├── Daily Raw Data.json
│   │   │   │   │   ├── {HH}/
│   │   │   │   │   │   ├── 4-hourly Raw Data.json
│   │
│   ├── processed/
│   │   ├── {data_layer}/  # e.g., silver, gold
│   │   │   ├── {data_source}/
│   │   │   │   ├── {table_name}/  # raw data 경로의 data_type에 해당됨
│   │   │   │   │   ├── {YYYY-MM-DD}/  # e.g., 2025-01-01
│   │   │   │   │   │   ├── Daily Transformed Data.snappy.parquet
│   │   │   │   │   │   ├── {HH}/
│   │   │   │   │   │   │   ├── 4-hourly Transformed Data.snappy.parquet
│
├── dags/
│   ├── {data_source}/
│   │   ├── bronze_DAG.py
│   │   ├── silver_DAG.py
│   │   ├── gold_DAG.py
│
├── scripts/
│   ├── {data_source}/
│   │   ├── script.py
```