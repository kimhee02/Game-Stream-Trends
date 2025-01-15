-- 기존 테이블 삭제 (이미 존재하는 경우)
DROP TABLE IF EXISTS silver.rawg_data;

-- 새로운 스키마에 맞게 테이블 재생성
CREATE TABLE silver.rawg_data (
    id INT,                   -- 게임 ID
    name VARCHAR(500),        -- 게임 이름
    released VARCHAR(50),            -- 출시일
    rating DOUBLE PRECISION,  -- 평점 평균
    ratings_count INT,        -- 전체 평점 개수
    reviews_count INT,        -- 리뷰 개수
    platforms VARCHAR(500),   -- 플랫폼 이름
    genres VARCHAR(500),      -- 장르 이름
    tags VARCHAR(1000)         -- 태그 이름
);

COPY silver.rawg_data
FROM 's3://gureum-bucket/data/processed/silver/rawg/'
IAM_ROLE 'arn:aws:iam::862327261051:role/gureum-redshift-role'
FORMAT AS PARQUET;