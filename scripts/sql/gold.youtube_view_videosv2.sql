-- 기존 테이블 삭제
DROP TABLE IF EXISTS gold.youtube_view_videosv2;

CREATE TABLE IF NOT EXISTS gold.youtube_view_videosv2 (
    video_id VARCHAR(255),                   -- 동영상 ID
    title VARCHAR(500),                      -- 동영상 제목
    tags VARCHAR(65535),                        -- 동영상 태그
    channel_title VARCHAR(255),              -- 채널 제목
    current_view_count BIGINT,               -- 최신 조회수
    previous_view_count BIGINT,              -- 이전 조회수
    view_count_change VARCHAR(255),          -- 조회수 변화량 (숫자 앞에 "+" 또는 "-" 포함)
    view_increase_percentage DOUBLE PRECISION, -- 조회수 증가율 (% 기호 없이 숫자로만 표시)
    current_collected_at TIMESTAMP,          -- 최신 수집 시점
    previous_collected_at TIMESTAMP         -- 이전 수집 시점
);

-- 데이터를 삽입하기 위한 쿼리
INSERT INTO gold.youtube_view_videosv2 (
    video_id, 
    title, 
    channel_title, 
    current_view_count, 
    previous_view_count, 
    view_count_change, 
    view_increase_percentage, 
    current_collected_at, 
    previous_collected_at, 
    tags
)
WITH latest_videos AS (
    -- 최신 날짜 및 시간의 데이터 가져오기
    SELECT 
        video_id, 
        title, 
        channel_title, 
        view_count AS current_view_count, 
        collected_at AS current_collected_at,
        tags,
        ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY collected_at DESC) AS row_num
    FROM 
        silver.youtube_videos
    WHERE 
        collected_at = (SELECT MAX(collected_at) FROM silver.youtube_videos) -- 가장 최신 데이터 선택
),
previous_videos AS (
    -- 하루 전 동일 시간 데이터 가져오기
    SELECT 
        video_id, 
        view_count AS previous_view_count, 
        collected_at AS previous_collected_at,
        ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY collected_at DESC) AS row_num
    FROM 
        silver.youtube_videos
    WHERE 
        DATE(collected_at) = (
            SELECT DATE(MAX(collected_at)) - INTERVAL '1 day' FROM silver.youtube_videos
        ) -- 전날 데이터 필터링
)
SELECT 
    l.video_id, 
    l.title, 
    l.channel_title, 
    l.current_view_count, 
    COALESCE(p.previous_view_count, 0) AS previous_view_count, -- 이전 조회수가 없으면 0으로 대체
    CASE 
        WHEN (l.current_view_count - COALESCE(p.previous_view_count, 0)) > 0 THEN 
            '+' || (l.current_view_count - COALESCE(p.previous_view_count, 0))::TEXT
        WHEN (l.current_view_count - COALESCE(p.previous_view_count, 0)) < 0 THEN 
            '-' || ABS(l.current_view_count - COALESCE(p.previous_view_count, 0))::TEXT
        ELSE 
            '0'
    END AS view_count_change, -- 조회수 변화량 (+, -, 또는 0으로 표시)
    CASE 
        WHEN COALESCE(p.previous_view_count, 0) > 0 THEN 
            ROUND((l.current_view_count - COALESCE(p.previous_view_count, 0))::NUMERIC / p.previous_view_count * 100, 2)
        ELSE 
            0
    END AS view_increase_percentage, -- 조회수 증가율 (% 기호 없이 숫자만 반환)
    l.current_collected_at, 
    p.previous_collected_at,
    l.tags
FROM 
    (SELECT * FROM latest_videos WHERE row_num = 1) l -- 최신 데이터 중 중복 제거
LEFT JOIN 
    (SELECT * FROM previous_videos WHERE row_num = 1) p -- 전날 데이터 중 중복 제거
ON 
    l.video_id = p.video_id -- video_id 기준으로 JOIN
ORDER BY 
    l.current_collected_at DESC, -- 최신 날짜 순으로 정렬
    l.current_view_count DESC; -- 조회수 순으로 정렬

