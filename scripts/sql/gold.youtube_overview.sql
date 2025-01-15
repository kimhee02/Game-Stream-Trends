CREATE TABLE IF NOT EXISTS gold.youtube_overview (
    title VARCHAR(500),
    channel_title VARCHAR(255),
    current_view_count BIGINT,
    view_count_change BIGINT,
    collected_at TIMESTAMP
);

INSERT INTO gold.youtube_overview
SELECT
    vv.title,
    vv.channel_title,
    vv.current_view_count,
    COALESCE(vv.current_view_count - vv.previous_view_count, 0) AS view_count_change,
    vv.current_collected_at AS collected_at
FROM gold.youtube_view_videosv2 vv
WHERE vv.current_view_count > 0; 

SELECT
    title,
    channel_title,
    current_view_count,
    view_count_change,
    CASE
        WHEN current_view_count > view_count_change AND view_count_change > 0 THEN
            ROUND((view_count_change * 100.0) / (current_view_count - view_count_change), 2)
        ELSE 0
    END AS view_growth_rate,
    collected_at
FROM
    gold.youtube_overview
WHERE
    current_view_count > 0 -- 조회수가 0 이상인 데이터만
ORDER BY
    current_view_count DESC
limit 10; -- 조회수 기준 내림차순 정렬
