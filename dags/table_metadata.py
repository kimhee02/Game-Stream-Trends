TABLES = {
    'steam': [
        {
            'category': 'players',
            'interval': '4-hourly',
            'table_name': 'steam_players',
            'staging_schema': """
                CREATE TABLE IF NOT EXISTS silver.steam_players_staging (
                    app_id VARCHAR(255),
                    player_count BIGINT,
                    result BIGINT,
                    collected_at TIMESTAMP
                );
            """,
            'columns': ['app_id', 'player_count', 'result', 'collected_at'],
            's3_path': 'steam/players',
            'join_condition': 's.app_id = t.app_id AND s.collected_at = t.collected_at',
            'unique_val': 'app_id'
        },
        {
            'category': 'review_metas',
            'interval': 'daily',
            'table_name': 'steam_review_metas',
            'staging_schema': """
                CREATE TABLE IF NOT EXISTS silver.steam_review_metas_staging (
                    app_id VARCHAR(255),
                    review_score BIGINT,
                    review_score_desc VARCHAR(255),
                    total_positive BIGINT,
                    total_negative BIGINT,
                    total_reviews BIGINT,
                    collected_at TIMESTAMP
                );
            """,
            'columns': ['app_id', 'review_score', 'review_score_desc', 'total_positive', 'total_negative', 'total_reviews', 'collected_at'],
            's3_path': 'steam/review_metas',
            'join_condition': 's.app_id = t.app_id AND s.collected_at = t.collected_at',
            'unique_val': 'app_id',
        },
    ],
    'twitch': [
        {
            'category': 'streams',
            'interval': '4-hourly',
            'table_name': 'twitch_streams',
            'staging_schema': """
                CREATE TABLE IF NOT EXISTS silver.twitch_streams_staging (
                    id VARCHAR(255),
                    user_name VARCHAR(255),
                    game_id VARCHAR(255),
                    game_name VARCHAR(255),
                    type VARCHAR(255),
                    title VARCHAR(500),
                    viewer_count BIGINT,
                    language VARCHAR(255),
                    is_mature boolean,
                    collected_at TIMESTAMP
                );
            """,
            'columns': ['id', 'user_name', 'game_id', 'game_name', 'type', 'title', 'viewer_count', 'language', 'is_mature', 'collected_at'],
            's3_path': 'twitch/streams',
            'join_condition': 's.id = t.id AND s.user_name = t.user_name AND s.collected_at = t.collected_at',
            'unique_val': 'id'
        },
        {
            'category': 'top_categories',
            'interval': '4-hourly',
            'table_name': 'twitch_top_categories',
            'staging_schema': """
                CREATE TABLE IF NOT EXISTS silver.twitch_top_categories_staging (
                    id VARCHAR(255),
                    name VARCHAR(255),
                    igdb_id VARCHAR(255),
                    rank INT,
                    collected_at TIMESTAMP
                );
            """,
            'columns': ['id', 'name', 'igdb_id', 'rank', 'collected_at'],
            's3_path': 'twitch/top_categories',
            'join_condition': 's.id = t.id AND s.collected_at = t.collected_at',
            'unique_val': 'id',
        },
    ],
    'youtube': [
        {
            'category': 'videos',
            'interval': '4-hourly',
            'table_name': 'youtube_videos',
            'staging_schema': """
                CREATE TABLE IF NOT EXISTS silver.youtube_videos_staging (
                    video_id VARCHAR(500),
                    published_at TIMESTAMP,
                    channel_id VARCHAR(500),
                    title VARCHAR(500),
                    channel_title VARCHAR(500),
                    tags VARCHAR(65535),
                    view_count BIGINT,
                    like_count BIGINT,
                    comment_count BIGINT,
                    collected_at TIMESTAMP
                );
            """,
            'columns': ['video_id', 'published_at', 'channel_id', 'title', 'channel_title', 'tags', 'view_count', 'like_count', 'comment_count', 'collected_at'],
            's3_path': 'youtube/videos',
            'join_condition': 's.video_id = t.video_id AND s.collected_at = t.collected_at',
            'unique_val': 'video_id'
        }
    ]
}
