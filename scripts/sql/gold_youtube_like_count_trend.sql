DROP TABLE IF EXISTS gold.youtube_like_count_trend;

-- 새로운 테이블 생성
CREATE TABLE gold.youtube_like_count_trend AS
WITH latest_data AS (
    -- 가장 최신 날짜 데이터 중 중복 제거
    SELECT 
        video_id, 
        title, 
        channel_title, 
        like_count AS current_like_count, 
        collected_at AS current_collected_at,
        ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY collected_at DESC) AS row_num -- 가장 최신 데이터를 선택
    FROM 
        silver.youtube_videos
    WHERE 
        collected_at = (SELECT MAX(collected_at) FROM silver.youtube_videos)
),
previous_data AS (
    -- 전날 데이터 중 중복 제거
    SELECT 
        video_id, 
        like_count AS previous_like_count, 
        collected_at AS previous_collected_at,
        ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY collected_at DESC) AS row_num -- 전날 가장 최신 데이터를 선택
    FROM 
        silver.youtube_videos
    WHERE 
        DATE(collected_at) = (SELECT DATE(MAX(collected_at)) - INTERVAL '1 day' FROM silver.youtube_videos)
)
SELECT 
    l.video_id, 
    l.title, 
    l.channel_title, 
    l.current_like_count, 
    p.previous_like_count, 
    COALESCE(l.current_like_count - p.previous_like_count, 0) AS like_count_change, -- NULL 대신 0 처리
    l.current_collected_at, 
    p.previous_collected_at
FROM 
    (SELECT * FROM latest_data WHERE row_num = 1) l -- 최신 데이터 중 중복 제거
LEFT JOIN 
    (SELECT * FROM previous_data WHERE row_num = 1) p -- 전날 데이터 중 중복 제거
ON 
    l.video_id = p.video_id -- video_id를 기준으로 JOIN
ORDER BY 
    like_count_change DESC;





--## 4시간마다 수집하니 중복된 데이터 발생 
-- WITH latest_data AS (
--     -- 가장 최신 날짜 데이터
--     SELECT 
--         video_id, 
--         title, 
--         channel_title, 
--         like_count AS current_like_count, 
--         collected_at AS current_collected_at
--     FROM 
--         silver.youtube_videos
--     WHERE 
--         collected_at = (SELECT MAX(collected_at) FROM silver.youtube_videos) -- 가장 최신 날짜
-- ),
-- previous_data AS (
--     -- 전날 데이터
--     SELECT 
--         video_id, 
--         like_count AS previous_like_count, 
--         collected_at AS previous_collected_at
--     FROM 
--         silver.youtube_videos
--     WHERE 
--         DATE(collected_at) = (SELECT DATE(MAX(collected_at)) - INTERVAL '1 day' FROM silver.youtube_videos) -- 전날 날짜
-- )
-- SELECT 
--     l.video_id, 
--     l.title, 
--     l.channel_title, 
--     l.current_like_count, 
--     p.previous_like_count, 
--     (l.current_like_count - p.previous_like_count) AS like_count_change, -- 좋아요 수 증가량
--     l.current_collected_at, 
--     p.previous_collected_at
-- FROM 
--     latest_data l
-- LEFT JOIN 
--     previous_data p
-- ON 
--     l.video_id = p.video_id -- video_id를 기준으로 JOIN
-- ORDER BY 
--     like_count_change DESC; 