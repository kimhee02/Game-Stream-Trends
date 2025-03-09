-- 기존 테이블 삭제
DROP TABLE IF EXISTS gold.youtube_like_count_trendv2;

CREATE TABLE gold.youtube_like_count_trendv2 (
    video_id VARCHAR(255),                    -- 동영상 ID
    title VARCHAR(500),                       -- 동영상 제목
    channel_title VARCHAR(255),               -- 채널 제목
    current_like_count BIGINT,                -- 최신 좋아요 수
    previous_like_count BIGINT,               -- 이전 좋아요 수
    like_count_change TEXT,                   -- 좋아요 수 변화량 (숫자 앞에 "+" 또는 "-" 포함)
    like_increase_percentage TEXT,            -- 좋아요 증가 비율 (퍼센트 기호 없이 문자열로 저장)
    current_comment_count BIGINT,             -- 최신 댓글 수
    previous_comment_count BIGINT,            -- 이전 댓글 수
    comment_count_change TEXT,                -- 댓글 수 변화량 (숫자 앞에 "+" 또는 "-" 포함)
    comment_increase_percentage TEXT,         -- 댓글 증가 비율 (퍼센트 기호 없이 문자열로 저장)
    current_view_count BIGINT,                -- 최신 조회수
    current_collected_at TIMESTAMP,           -- 최신 수집 시점
    previous_collected_at TIMESTAMP           -- 이전 수집 시점
);

-- 데이터 삽입
INSERT INTO gold.youtube_like_count_trendv2 (
    video_id, 
    title, 
    channel_title, 
    current_like_count, 
    previous_like_count, 
    like_count_change, 
    like_increase_percentage, 
    current_comment_count, 
    previous_comment_count, 
    comment_count_change, 
    comment_increase_percentage, 
    current_view_count, 
    current_collected_at, 
    previous_collected_at
)
WITH latest_data AS (
    SELECT 
        video_id, 
        title, 
        channel_title, 
        COALESCE(like_count, 0) AS current_like_count, -- NULL 값을 0으로 대체
        COALESCE(comment_count, 0) AS current_comment_count, -- NULL 값을 0으로 대체
        COALESCE(view_count, 0) AS current_view_count, -- NULL 값을 0으로 대체
        collected_at::TIMESTAMP AS current_collected_at, -- 명시적으로 TIMESTAMP로 캐스팅
        ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY collected_at DESC) AS row_num
    FROM 
        silver.youtube_videos
    WHERE 
        collected_at = (SELECT MAX(collected_at) FROM silver.youtube_videos)
),
previous_data AS (
    SELECT 
        video_id, 
        COALESCE(like_count, 0) AS previous_like_count, -- NULL 값을 0으로 대체
        COALESCE(comment_count, 0) AS previous_comment_count, -- NULL 값을 0으로 대체
        collected_at::TIMESTAMP AS previous_collected_at, -- 명시적으로 TIMESTAMP로 캐스팅
        ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY collected_at DESC) AS row_num
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
    COALESCE(p.previous_like_count, 0) AS previous_like_count, -- NULL 값을 0으로 대체
    CASE 
        WHEN l.current_like_count - COALESCE(p.previous_like_count, 0) > 0 THEN 
            '+' || (l.current_like_count - COALESCE(p.previous_like_count, 0))::TEXT
        ELSE 
            (l.current_like_count - COALESCE(p.previous_like_count, 0))::TEXT
    END AS like_count_change, -- 좋아요 변화량 (+ 기호 포함)
    CASE 
        WHEN p.previous_like_count > 0 THEN 
            ROUND((l.current_like_count - p.previous_like_count)::NUMERIC / p.previous_like_count * 100, 2)::TEXT
        ELSE 
            '0'
    END AS like_increase_percentage, -- 좋아요 증가율 (숫자만 출력)
    l.current_comment_count, 
    COALESCE(p.previous_comment_count, 0) AS previous_comment_count, -- NULL 값을 0으로 대체
    CASE 
        WHEN l.current_comment_count - COALESCE(p.previous_comment_count, 0) > 0 THEN 
            '+' || (l.current_comment_count - COALESCE(p.previous_comment_count, 0))::TEXT
        ELSE 
            (l.current_comment_count - COALESCE(p.previous_comment_count, 0))::TEXT
    END AS comment_count_change, -- 댓글 변화량 (+ 기호 포함)
    CASE 
        WHEN p.previous_comment_count > 0 THEN 
            ROUND((l.current_comment_count - p.previous_comment_count)::NUMERIC / p.previous_comment_count * 100, 2)::TEXT
        ELSE 
            '0'
    END AS comment_increase_percentage, -- 댓글 증가율 (숫자만 출력)
    l.current_view_count,
    l.current_collected_at, -- TIMESTAMP 값
    p.previous_collected_at -- TIMESTAMP 값
FROM 
    (SELECT * FROM latest_data WHERE row_num = 1) l
LEFT JOIN 
    (SELECT * FROM previous_data WHERE row_num = 1) p
ON 
    l.video_id = p.video_id
ORDER BY 
    l.current_like_count DESC; -- current_like_count 기준 정렬