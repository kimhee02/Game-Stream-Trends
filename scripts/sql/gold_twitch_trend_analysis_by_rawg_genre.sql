DROP TABLE IF EXISTS gold_twitch_trend_analysis_by_rawg_genre;
CREATE TABLE gold_twitch_trend_analysis_by_rawg_genre AS

WITH RankedCategories AS (
    SELECT 
        tc.id AS category_id,          
        rd.name AS game_name,          
        rd.genres AS rawg_genres,      
        tc.rank AS rank,               
        ROW_NUMBER() OVER (PARTITION BY tc.id ORDER BY tc.rank ASC) AS rnk
    FROM 
        silver.twitch_top_categories tc
    LEFT JOIN 
        silver.rawg_data rd
    ON 
        LOWER(tc.name) = LOWER(rd.name)
    WHERE
        tc.id IS NOT NULL
        AND rd.id IS NOT NULL
)
SELECT 
    category_id, 
    game_name, 
    rawg_genres, 
    rank
FROM 
    RankedCategories
WHERE 
    rnk = 1 -- 각 카테고리 ID별로 순위가 가장 높은 데이터만 선택
ORDER BY 
    rank ASC;



-- SELECT 
--     rd.genres AS rawg_genres,                -- RAWG 장르
--     COUNT(tc.id) AS category_count,          -- 해당 장르에 속한 Twitch 카테고리 수
--     AVG(tc.rank) AS avg_twitch_rank          -- 해당 장르의 평균 Twitch 순위
-- FROM 
--     silver.twitch_top_categories tc
-- INNER JOIN 
--     silver.rawg_data rd
-- ON 
--     LOWER(tc.name) = LOWER(rd.name)          -- 이름 기준으로 매칭
-- WHERE 
--     tc.rank IS NOT NULL                      -- Twitch 순위가 NULL이 아닌 경우만
-- GROUP BY 
--     rd.genres
-- HAVING 
--     rd.genres IS NOT NULL                    -- NULL 값 제거
--     AND rd.genres != 'NULL'                  -- 'NULL' 문자열 제거
--     AND rd.genres != ''                     -- 빈 문자열 제거
-- ORDER BY 
--     category_count DESC;




-- -- -- ##전체 출력
-- SELECT DISTINCT
--     tc.id AS category_id,                    
--     tc.name AS category_name,              
--     rd.id AS game_id,                       
--     rd.name AS game_name,                    
--     rd.genres AS rawg_genres,                
--     tc.rank AS twitch_rank,                  
--     tc.collected_at AS collected_at          
-- FROM 
--     silver.twitch_top_categories tc
-- LEFT JOIN 
--     silver.rawg_data rd
-- ON 
--     LOWER(tc.name) = LOWER(rd.name)          
-- WHERE 
--     tc.id IS NOT NULL                        
--     AND rd.id IS NOT NULL;                  



-- -- rawg의 동일한 게임이름이 여러개 있음 
-- SELECT 
--     name, 
--     COUNT(*) AS count
-- FROM 
--     silver.rawg_data
-- GROUP BY 
--     name
-- HAVING 
--     COUNT(*) > 1
-- ORDER BY 
--     count DESC;