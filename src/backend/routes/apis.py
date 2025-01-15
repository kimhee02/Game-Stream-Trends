from fastapi import APIRouter
from utils.dbclient import DB

router = APIRouter()
db = DB()


@router.get('/api/test')
async def get_test():
    query = 'SELECT current_date'
    result = db.execute_query(query)
    return {'result': result}


@router.get('/api/steam/overview')
async def get_steam_summary():
    query = 'SELECT * FROM gold.steam_overview ORDER BY steam_player_count DESC LIMIT 10'
    result = db.execute_query(query)
    return {'result': result}


@router.get('/api/steam/popular')
async def get_steam_popular():
    query = 'SELECT * FROM gold.steam_popular_games_v2'
    result = db.execute_query(query)
    return {'result': result}


@router.get('/api/steam/recommend')
async def get_steam_recommend():
    query = 'SELECT * FROM gold.recommend_steam_top_games_v2'
    result = db.execute_query(query)
    return {'result': result}


@router.get('/api/youtube/overview')
async def get_youtube_overview():
    query = 'SELECT * FROM gold.youtube_overview ORDER BY current_view_count DESC LIMIT 10'
    result = db.execute_query(query)
    return {'result': result}


@router.get('/api/youtube/view')
async def get_youtube_like():
    query = 'SELECT * FROM gold.youtube_view_videosv2 ORDER BY current_collected_at DESC, current_view_count DESC;'
    result = db.execute_query(query)
    return {'result': result}


@router.get('/api/youtube/like')
async def get_youtube_view():
    query = 'SELECT * FROM gold.youtube_like_count_trendv2 ORDER BY current_collected_at DESC, current_view_count DESC;'
    result = db.execute_query(query)
    return {'result': result}


@router.get('/api/twitch/overview')
async def get_twitch_overview():
    query = 'SELECT * FROM gold.twitch_overview ORDER BY twitch_viewer_count DESC LIMIT 10'
    result = db.execute_query(query)
    return {'result': result}


@router.get('/api/twitch/game')
async def get_twitch_game():
    query = 'SELECT * FROM gold.twitch_game_summary_v2 LIMIT 100'
    result = db.execute_query(query)
    return {'result': result}