DROP TABLE IF EXISTS gold.youtube_twitch_viewer_analysis;

CREATE TABLE gold.youtube_twitch_viewer_analysis AS


SELECT
    ts.game_id AS game_id,
    COALESCE(ts.game_name, MIN(yv.tags)) AS game_name,
    MAX(ts.viewer_count) AS twitch_viewer_count,      
    SUM(yv.view_count) AS youtube_view_count,         
    SUM(yv.like_count) AS youtube_like_count,          
    SUM(yv.comment_count) AS youtube_comment_count,    
    BOOL_OR(ts.is_mature) AS is_mature,               
    MIN(LEAST(ts.collected_at, yv.collected_at)) AS collected_at 
FROM 
    silver.twitch_streams ts
JOIN 
    silver.youtube_videos yv
ON 
    LOWER(yv.tags) LIKE '%' || LOWER(ts.game_name) || '%'
WHERE 
    ts.game_id IS NOT NULL 
    AND ts.game_name IS NOT NULL 
    AND yv.video_id IS NOT NULL 
    AND yv.tags IS NOT NULL
GROUP BY 
    ts.game_id, ts.game_name;



-- ## 전체 출력
-- SELECT DISTINCT
--     ts.game_id AS game_id,
--     COALESCE(ts.game_name, yv.tags) AS game_name,
--     ts.viewer_count AS twitch_viewer_count,
--     yv.view_count AS youtube_view_count,
--     yv.like_count AS youtube_like_count,
--     yv.comment_count AS youtube_comment_count,
--     ts.is_mature AS is_mature,
--     LEAST(ts.collected_at, yv.collected_at) AS collected_at
-- FROM 
--     silver.twitch_streams ts
-- JOIN 
--     silver.youtube_videos yv
-- ON 
--     LOWER(yv.tags) LIKE '%' || LOWER(ts.game_name) || '%'
-- WHERE 
--     ts.game_id IS NOT NULL 
--     AND ts.game_name IS NOT NULL 
--     AND yv.video_id IS NOT NULL 
--     AND yv.tags IS NOT NULL
--     AND COALESCE(ts.game_name, yv.tags) IS NOT NULL
--     AND ts.game_id IS NOT NULL;