TABLES = {
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
                    title VARCHAR(255),
                    viewer_count BIGINT,
                    language VARCHAR(255),
                    is_mature boolean,
                    collected_at TIMESTAMP
                );
            """,
            'columns': ['id', 'user_name', 'game_id', 'game_name', 'type', 'title', 'viewer_count', 'language', 'is_mature', 'collected_at'],
            's3_path': 'twitch/streams',
            'join_condition': 's.id = t.id AND s.collected_at = t.collected_at',
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
                    collected_at TIMESTAMP
                );
            """,
            'columns': ['id', 'name', 'igdb_id', 'collected_at'],
            's3_path': 'twitch/top_categories',
            'join_condition': 's.id = t.id AND s.collected_at = t.collected_at',
            'unique_val': 'id',
        },
    ]
}