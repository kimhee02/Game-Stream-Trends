CREATE TABLE IF NOT EXISTS gold.twitch_game_summary_v2 (
    game_name VARCHAR(255),
    top_streamer VARCHAR(255),
    total_streams BIGINT,
    total_viewers BIGINT,
    viewer_increase BIGINT,
    is_mature_content BOOLEAN,
    collected_at TIMESTAMP
);

INSERT INTO gold.twitch_game_summary_v2
SELECT
    current_data.game_name,
    current_data.top_streamer,
    current_data.total_streams,
    current_data.total_viewers,
    CASE
        WHEN previous_data.total_viewers IS NOT NULL THEN current_data.total_viewers - previous_data.total_viewers
        ELSE NULL
    END AS viewer_increase, -- 직전 데이터가 없으면 NULL
    current_data.is_mature_content,
    current_data.collected_at
FROM (
    -- 현재 데이터: 가장 최근 수집된 데이터
    SELECT
        ts.game_id,
        ts.game_name,
        COUNT(ts.id) AS total_streams,
        SUM(ts.viewer_count) AS total_viewers,
        MAX(ts.is_mature::INT)::BOOLEAN AS is_mature_content,
        (
            SELECT ts_inner.user_name
            FROM silver.twitch_streams ts_inner
            WHERE ts_inner.game_id = ts.game_id
                AND DATE_TRUNC('minute', ts_inner.collected_at) = DATE_TRUNC('minute', MAX(ts.collected_at))
            ORDER BY ts_inner.viewer_count DESC
            LIMIT 1
        ) AS top_streamer,
        MAX(ts.collected_at) AS collected_at
    FROM silver.twitch_streams ts
    WHERE DATE_TRUNC('hour', ts.collected_at) = DATE_TRUNC('hour', (SELECT MAX(collected_at) FROM silver.twitch_streams))
        AND LOWER(TRIM(ts.game_name)) NOT IN (
            'just chatting', 
            'i\'m only sleeping', 
            'music', 
            'asmr', 
            'talk shows & podcasts', 
            'science & technology',
            'always on',
            'special events',
            'sports',
            'retro',
            'djs',
            'art'
        )
        AND ts.game_name ~ '^[a-zA-Z0-9 ]+$'
        AND ts.viewer_count > 100
    GROUP BY ts.game_id, ts.game_name
) current_data
LEFT JOIN (
    -- 이전 데이터: 정확히 4시간 전의 데이터를 가져오기
    SELECT
        ts.game_id,
        ts.game_name,
        SUM(ts.viewer_count) AS total_viewers,
        MAX(ts.collected_at) AS collected_at
    FROM silver.twitch_streams ts
    WHERE DATE_TRUNC('hour', ts.collected_at) = DATE_TRUNC('hour', DATEADD('hour', -4, (SELECT MAX(collected_at) FROM silver.twitch_streams)))
    GROUP BY ts.game_id, ts.game_name
) previous_data
ON current_data.game_id = previous_data.game_id
    AND current_data.game_name = previous_data.game_name
WHERE current_data.total_viewers > 0 -- 현재 데이터가 유효한 경우만 삽입
ORDER BY viewer_increase DESC NULLS LAST, current_data.total_viewers DESC;

SELECT * FROM gold.twitch_game_summary_v2
ORDER BY total_viewers DESC
LIMIT 50;