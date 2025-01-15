-- Create steam overview
CREATE TABLE IF NOT EXISTS gold.steam_overview (
    game_name VARCHAR(255),
    steam_player_count BIGINT,
    steam_player_increase BIGINT,
    collected_at TIMESTAMP
);

-- Insert into steam_overview
INSERT INTO gold.steam_overview
SELECT
    sl.app_name AS game_name,
    sp.player_count AS steam_player_count,
    (sp.player_count - COALESCE(prev_data.player_count, 0)) AS steam_player_increase,
    sp.collected_at
FROM
    silver.steam_players sp
LEFT JOIN silver.steam_game_list sl
    ON sp.app_id = sl.app_id
LEFT JOIN (
    SELECT
        sp.app_id,
        sp.player_count,
        sp.collected_at
    FROM
        silver.steam_players sp
    WHERE
        sp.collected_at = (
            SELECT MAX(collected_at)
            FROM silver.steam_players
            WHERE collected_at < (SELECT MAX(collected_at) FROM silver.steam_players)
        )
) prev_data
    ON sp.app_id = prev_data.app_id
WHERE
    sp.collected_at = (SELECT MAX(collected_at) FROM silver.steam_players)
    AND sl.app_name IS NOT NULL 
ORDER BY
    steam_player_increase DESC;

-- Steam 데이터 조회 쿼리(증가율 포함)
SELECT
    game_name,
    steam_player_count,
    steam_player_increase,
    CASE
        WHEN steam_player_increase IS NOT NULL AND steam_player_increase <> 0 THEN
            ROUND((steam_player_increase * 100.0) / (steam_player_count - steam_player_increase), 2) -- 증가율 계산
        ELSE 0
    END AS steam_growth_rate,
    collected_at
FROM
    gold.steam_overview
ORDER BY
    steam_player_count DESC
limit 10; -- 플레이어 수 기준 내림차순 정렬