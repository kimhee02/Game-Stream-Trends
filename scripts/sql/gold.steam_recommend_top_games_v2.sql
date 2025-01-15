CREATE TABLE IF NOT EXISTS gold.steam_recommend_top_games_v2 (
    game_name VARCHAR(255),
    header_image VARCHAR(255),
    positive_review_ratio DOUBLE PRECISION,
    positive_review_increase BIGINT,
    negative_review_increase BIGINT,
    total_positive BIGINT, 
    total_negative BIGINT, 
    collected_at TIMESTAMP
);

INSERT INTO gold.steam_recommend_top_games_v2
SELECT
    current_data.game_name,
    si.header_image, -- steam_images 테이블에서 가져온 header_image URL
    current_data.positive_review_ratio,
    CASE
        WHEN previous_data.total_positive IS NOT NULL THEN current_data.total_positive - previous_data.total_positive
        ELSE NULL
    END AS positive_review_increase,
    CASE
        WHEN previous_data.total_negative IS NOT NULL THEN current_data.total_negative - previous_data.total_negative
        ELSE NULL
    END AS negative_review_increase,
    current_data.total_positive,
    current_data.total_negative,
    current_data.collected_at
FROM (
    -- 현재 데이터: 가장 최근 수집된 데이터
    SELECT
        sg.app_name AS game_name,
        CASE
            WHEN rm.total_reviews > 0 THEN (rm.total_positive::DOUBLE PRECISION / rm.total_reviews) * 100
            ELSE 0
        END AS positive_review_ratio,
        rm.total_positive,
        rm.total_negative,
        rm.collected_at,
        sg.app_id -- steam_images와 JOIN을 위해 app_id를 포함
    FROM silver.steam_game_list sg
    LEFT JOIN silver.steam_review_metas rm
        ON sg.app_id = rm.app_id
    WHERE rm.collected_at = (SELECT MAX(collected_at) FROM silver.steam_review_metas) -- 가장 최근 데이터
) current_data
LEFT JOIN (
    -- 이전 데이터: 가장 최근 데이터 바로 전날
    SELECT
        sg.app_name AS game_name,
        rm.total_positive,
        rm.total_negative,
        rm.collected_at,
        sg.app_id -- steam_images와 JOIN을 위해 app_id를 포함
    FROM silver.steam_game_list sg
    LEFT JOIN silver.steam_review_metas rm
        ON sg.app_id = rm.app_id
    WHERE rm.collected_at = (
        SELECT MAX(collected_at)
        FROM silver.steam_review_metas
        WHERE collected_at < (SELECT MAX(collected_at) FROM silver.steam_review_metas) -- 전날 데이터
    )
) previous_data
ON current_data.game_name = previous_data.game_name
LEFT JOIN silver.steam_images si
    ON current_data.app_id = si.app_id -- 이미지 URL을 가져오기 위한 JOIN
ORDER BY positive_review_increase DESC;

SELECT * 
FROM gold.steam_recommend_top_games_v2
ORDER BY positive_review_ratio DESC, positive_review_increase DESC;