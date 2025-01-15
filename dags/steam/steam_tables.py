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
    ]
}