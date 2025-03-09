
DROP TABLE IF EXISTS gold.youtube_view_videos;

CREATE TABLE gold.youtube_view_videos AS

WITH latest_videos AS (
    SELECT 
        video_id, 
        title, 
        channel_title, 
        view_count AS current_view_count, 
        like_count AS current_like_count, 
        comment_count AS current_comment_count, 
        tags, 
        collected_at AS current_collected_at,
        ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY collected_at DESC) AS row_num -- 가장 최신 데이터 선택
    FROM 
        silver.youtube_videos
    WHERE 
        collected_at = (SELECT MAX(collected_at) FROM silver.youtube_videos) -- 가장 최신 날짜만 선택
),
previous_videos AS (
    SELECT 
        video_id, 
        view_count AS previous_view_count, 
        collected_at AS previous_collected_at,
        ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY collected_at DESC) AS row_num -- 전날 데이터 선택
    FROM 
        silver.youtube_videos
    WHERE 
        DATE(collected_at) = (SELECT DATE(MAX(collected_at)) - INTERVAL '1 day' FROM silver.youtube_videos) -- 전날 데이터
)
SELECT 
    l.video_id, 
    l.title, 
    l.channel_title, 
    l.current_view_count, 
    COALESCE(p.previous_view_count, 0) AS previous_view_count, -- NULL은 0으로 대체
    (l.current_view_count - COALESCE(p.previous_view_count, 0)) AS view_count_change, -- 조회수 증가량
    l.current_like_count, 
    l.current_comment_count, 
    l.tags, 
    l.current_collected_at, 
    p.previous_collected_at
FROM 
    (SELECT * FROM latest_videos WHERE row_num = 1) l -- 최신 데이터 중 중복 제거
LEFT JOIN 
    (SELECT * FROM previous_videos WHERE row_num = 1) p -- 전날 데이터 중 중복 제거
ON 
    l.video_id = p.video_id -- video_id 기준으로 JOIN
ORDER BY 
    current_view_count DESC; -- 현재 조회수 기준 정렬








-- WITH latest_videos AS (
--     SELECT 
--         video_id, 
--         title, 
--         channel_title, 
--         view_count, 
--         like_count, 
--         comment_count, 
--         tags, 
--         published_at, 
--         collected_at,
--         ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY collected_at DESC, view_count DESC) AS row_num
--     FROM 
--         silver.youtube_videos
-- )
-- SELECT 
--     video_id, 
--     title, 
--     channel_title, 
--     view_count, 
--     like_count, 
--     comment_count, 
--     tags, 
--     published_at, 
--     collected_at
-- FROM 
--     latest_videos
-- WHERE 
--     row_num = 1
-- ORDER BY 
--     collected_at DESC, 
--     view_count DESC;


-- WITH latest_videos AS (
--     SELECT 
--         video_id, 
--         title, 
--         channel_title, 
--         view_count, 
--         like_count, 
--         comment_count, 
--         tags, 
--         published_at, 
--         collected_at,
--         ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY collected_at DESC, view_count DESC) AS row_num -- 최신 날짜와 조회수 기준으로 순위 부여
--     FROM 
--         silver.youtube_videos
-- )
-- SELECT 
--     video_id, 
--     title, 
--     channel_title, 
--     view_count, 
--     like_count, 
--     comment_count, 
--     tags, 
--     published_at, 
--     collected_at
-- FROM 
--     latest_videos
-- WHERE 
--     row_num = 1 -- video_id별로 가장 최신 데이터만 선택
-- ORDER BY 
--     collected_at DESC, -- 수집 날짜와 시간 기준으로 정렬
--     view_count DESC; -- 동일한 시간 내에서 조회수 기준으로 정렬






-- --전체 게임id 
-- SELECT 
--     video_id, 
--     title, 
--     channel_title, 
--     view_count, 
--     like_count,
--     comment_count, 
--     tags, 
--     published_at, 
--     collected_at
-- FROM 
--     silver.youtube_videos
-- ORDER BY 
--     view_count DESC; -- 전체 데이터를 조회수 기준으로 정렬









