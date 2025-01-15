CREATE TABLE IF NOT EXISTS gold.steam_popular_games_v2 (
    game_name VARCHAR(255),
    steam_app_id VARCHAR(255),
    header_image VARCHAR(255),
    player_count BIGINT,
    player_count_increase BIGINT,
    review_score BIGINT,
    positive_review_ratio DOUBLE PRECISION,
    collected_at TIMESTAMP
);

INSERT INTO gold.steam_popular_games_v2
SELECT
    current_data.game_name,
    current_data.steam_app_id,
    si.header_image, -- steam_images 테이블에서 가져온 header_image URL
    current_data.player_count,
    CASE
        WHEN previous_data.player_count IS NOT NULL THEN current_data.player_count - previous_data.player_count
        ELSE NULL
    END AS player_count_increase,
    review_data.review_score,
    review_data.positive_review_ratio,
    current_data.collected_at
FROM (
    -- 현재 데이터: steam_players에서 가장 최근 데이터
    SELECT
        sg.app_name AS game_name,
        sg.app_id AS steam_app_id,
        sp.player_count,
        sp.collected_at
    FROM silver.steam_game_list sg
    LEFT JOIN silver.steam_players sp
        ON sg.app_id = sp.app_id
    WHERE sp.collected_at = (SELECT MAX(collected_at) FROM silver.steam_players) -- 가장 최근 데이터
) current_data
LEFT JOIN (
    -- 이전 데이터: steam_players에서 가장 최근 데이터 직전 데이터 
    SELECT
        sg.app_name AS game_name,
        sg.app_id AS steam_app_id,
        sp.player_count,
        sp.collected_at
    FROM silver.steam_game_list sg
    LEFT JOIN silver.steam_players sp
        ON sg.app_id = sp.app_id
    WHERE sp.collected_at = (
        SELECT MAX(collected_at)
        FROM silver.steam_players
        WHERE collected_at < (SELECT MAX(collected_at) FROM silver.steam_players) -- 바로 이전 데이터
    )
) previous_data
ON current_data.steam_app_id = previous_data.steam_app_id
LEFT JOIN (
    -- steam_review_metas 데이터와 동기화: 가장 가까운 collected_at 값을 찾기
    SELECT
        rm.app_id AS steam_app_id,
        rm.review_score,
        CASE
            WHEN rm.total_reviews > 0 THEN (rm.total_positive::DOUBLE PRECISION / rm.total_reviews) * 100
            ELSE 0
        END AS positive_review_ratio,
        rm.collected_at
    FROM silver.steam_review_metas rm
    WHERE rm.collected_at = (
        SELECT MAX(collected_at) FROM silver.steam_review_metas -- 가장 최근 수집된 리뷰 데이터
    )
) review_data
ON current_data.steam_app_id = review_data.steam_app_id
LEFT JOIN silver.steam_images si
ON current_data.steam_app_id = si.app_id -- steam_images와 JOIN하여 header_image 추가
ORDER BY current_data.player_count DESC;

-- 순위 조회 쿼리
WITH ranked_games AS (
    SELECT
        RANK() OVER (PARTITION BY collected_at ORDER BY player_count DESC) AS popularity_rank,
        game_name,
        steam_app_id,
        header_image,
        player_count,
        player_count_increase,
        review_score,
        positive_review_ratio,
        collected_at
    FROM gold.steam_popular_games_v2
)
SELECT DISTINCT
    popularity_rank,
    game_name,
    steam_app_id,
    header_image,
    player_count,
    player_count_increase,
    review_score,
    positive_review_ratio,
    collected_at
FROM ranked_games
ORDER BY collected_at, popularity_rank;
