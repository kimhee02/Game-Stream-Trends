CREATE TABLE IF NOT EXISTS gold.twitch_overview (
    game_name VARCHAR(255),
    twitch_viewer_count BIGINT,
    twitch_viewer_increase BIGINT,
    collected_at TIMESTAMP
);

INSERT INTO gold.twitch_overview
SELECT
    game_name,
    total_viewers AS twitch_viewer_count,
    viewer_increase AS twitch_viewer_increase,
    collected_at
FROM
    gold.twitch_game_summary_v2
WHERE
    total_viewers > 0 -- 총 시청자가 유효한 경우만
    AND collected_at = (SELECT MAX(collected_at) FROM gold.twitch_game_summary_v2) -- 최신 데이터만 가져오기
ORDER BY
    total_viewers DESC;

SELECT
    game_name,
    twitch_viewer_count,
    twitch_viewer_increase,
    CASE
        WHEN twitch_viewer_increase IS NOT NULL AND twitch_viewer_increase <> 0 THEN
            ROUND((twitch_viewer_increase * 100.0) / (twitch_viewer_count - twitch_viewer_increase), 2) -- 증가율 계산
        ELSE 0
    END AS twitch_growth_rate,
    collected_at
FROM
    gold.twitch_overview
ORDER BY
    twitch_viewer_count DESC
limit 10;
