-- Chess Production Stored Procedures
-- Database: DLT_SNOWPARK_SFRT
-- Schema: CHESS_PRODUCTION
-- These procedures demonstrate dlt production patterns with Chess.com API
-- Based on: https://dlthub.com/docs/examples/chess_production
-- Refactored for Snowflake stored procedures using DltSnowparkLoader

-- ============================================================
-- PREREQUISITES: Run these commands once to set up network access
-- ============================================================
-- CREATE SCHEMA IF NOT EXISTS DLT_SNOWPARK_SFRT.CHESS_PRODUCTION;
-- 
-- CREATE OR REPLACE NETWORK RULE DLT_SNOWPARK_SFRT.CHESS_PRODUCTION.CHESS_API_NETWORK_RULE
--   MODE = EGRESS
--   TYPE = HOST_PORT
--   VALUE_LIST = ('api.chess.com:443');
-- 
-- CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION CHESS_API_ACCESS_INTEGRATION
--   ALLOWED_NETWORK_RULES = (DLT_SNOWPARK_SFRT.CHESS_PRODUCTION.CHESS_API_NETWORK_RULE)
--   ENABLED = TRUE
--   COMMENT = 'Allows stored procedures to access Chess.com API';
-- ============================================================

-- ============================================================
-- Procedure 1: P_CHESS_PLAYERS_BASIC
-- Basic example: Fetch titled players from Chess.com
-- ============================================================
CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.CHESS_PRODUCTION."P_CHESS_PLAYERS_BASIC"(
    "TITLE" VARCHAR DEFAULT 'GM',
    "MAX_PLAYERS" INTEGER DEFAULT 5
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python','dlt','requests')
HANDLER = 'main'
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
EXTERNAL_ACCESS_INTEGRATIONS = (CHESS_API_ACCESS_INTEGRATION)
EXECUTE AS OWNER
AS '
def main(session, title: str, max_players: int):
    """
    Basic Chess Players Example
    
    Fetches titled players from Chess.com API.
    Similar to the players() resource in the dlt chess_production example.
    
    Args:
        title: Player title (GM, IM, FM, etc.)
        max_players: Maximum number of players to fetch
    """
    import dlt
    import json
    import requests
    from dlt_snowpark import DltSnowparkLoader
    
    CHESS_URL = "https://api.chess.com/pub"
    HEADERS = {"User-Agent": "dlt-snowpark-chess-pipeline/1.0 (Snowflake SP)"}
    loader = DltSnowparkLoader(session, pipeline_name="chess_players_basic")
    
    # Fetch titled players from Chess.com API
    response = requests.get(f"{CHESS_URL}/titled/{title}", headers=HEADERS)
    response.raise_for_status()
    players_data = response.json()["players"][:max_players]
    
    @dlt.resource(name="players", write_disposition="replace")
    def get_players():
        """Yields player usernames with metadata"""
        processed = []
        for idx, username in enumerate(players_data):
            processed.append({
                "username": username,
                "title": title,
                "list_position": idx + 1,
            })
        yield processed
    
    result = loader.run(get_players())
    verification = loader.verify_table("players")
    
    return json.dumps({
        "load_result": result,
        "verification": verification,
        "title": title,
        "players_count": len(players_data)
    }, indent=2, default=str)
';

-- ============================================================
-- Procedure 2: P_CHESS_PLAYERS_PROFILES
-- Transformer pattern: Fetch player profiles from Chess.com
-- Equivalent to: players | players_profiles
-- ============================================================
CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.CHESS_PRODUCTION."P_CHESS_PLAYERS_PROFILES"(
    "TITLE" VARCHAR DEFAULT 'GM',
    "MAX_PLAYERS" INTEGER DEFAULT 5
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python','dlt','requests')
HANDLER = 'main'
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
EXTERNAL_ACCESS_INTEGRATIONS = (CHESS_API_ACCESS_INTEGRATION)
EXECUTE AS OWNER
AS '
def main(session, title: str, max_players: int):
    """
    Chess Players Profiles with Transformer Pattern
    
    Implements the dlt transformer pattern:
    - players: Base resource (not loaded separately)
    - players_profiles: Transformer that fetches profile for each player
    
    Equivalent to: players | players_profiles (parallelized=True)
    """
    import dlt
    import json
    import requests
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from dlt_snowpark import DltSnowparkLoader
    
    CHESS_URL = "https://api.chess.com/pub"
    HEADERS = {"User-Agent": "dlt-snowpark-chess-pipeline/1.0 (Snowflake SP)"}
    loader = DltSnowparkLoader(session, pipeline_name="chess_players_profiles")
    
    # Fetch titled players (this is the players() resource)
    response = requests.get(f"{CHESS_URL}/titled/{title}", headers=HEADERS)
    response.raise_for_status()
    players_list = response.json()["players"][:max_players]
    
    def fetch_profile(username):
        """Fetch player profile - equivalent to @dlt.transformer with parallelized=True"""
        try:
            response = requests.get(f"{CHESS_URL}/player/{username}", headers=HEADERS)
            response.raise_for_status()
            return response.json()
        except requests.HTTPError:
            return None
    
    # Use ThreadPoolExecutor for parallel fetching (like @dlt.transformer(parallelized=True))
    profiles_list = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_username = {
            executor.submit(fetch_profile, username): username 
            for username in players_list
        }
        for future in as_completed(future_to_username):
            username = future_to_username[future]
            try:
                profile = future.result()
                if profile:
                    profiles_list.append(profile)
            except Exception:
                pass
    
    @dlt.resource(name="players_profiles", write_disposition="replace")
    def get_profiles():
        """Yields flattened player profiles"""
        flattened = []
        for profile in profiles_list:
            flat_profile = {
                "player_id": profile.get("player_id"),
                "username": profile.get("username"),
                "title": profile.get("title"),
                "name": profile.get("name"),
                "country": profile.get("country"),
                "location": profile.get("location"),
                "followers": profile.get("followers"),
                "joined": profile.get("joined"),
                "last_online": profile.get("last_online"),
                "status": profile.get("status"),
                "is_streamer": profile.get("is_streamer"),
                "verified": profile.get("verified"),
                "league": profile.get("league"),
                "avatar": profile.get("avatar"),
                "url": profile.get("url"),
            }
            flattened.append(flat_profile)
        yield flattened
    
    result = loader.run(get_profiles())
    verification = loader.verify_table("players_profiles")
    
    return json.dumps({
        "load_result": result,
        "verification": verification,
        "title": title,
        "profiles_fetched": len(profiles_list),
        "note": "Used ThreadPoolExecutor for parallel profile fetching (like @dlt.transformer parallelized=True)"
    }, indent=2, default=str)
';

-- ============================================================
-- Procedure 3: P_CHESS_PLAYERS_GAMES
-- Fetch games for players in a specific month
-- Equivalent to: players | players_games
-- ============================================================
CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.CHESS_PRODUCTION."P_CHESS_PLAYERS_GAMES"(
    "TITLE" VARCHAR DEFAULT 'GM',
    "MAX_PLAYERS" INTEGER DEFAULT 3,
    "YEAR" INTEGER DEFAULT 2024,
    "MONTH" INTEGER DEFAULT 12
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python','dlt','requests')
HANDLER = 'main'
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
EXTERNAL_ACCESS_INTEGRATIONS = (CHESS_API_ACCESS_INTEGRATION)
EXECUTE AS OWNER
AS '
def main(session, title: str, max_players: int, year: int, month: int):
    """
    Chess Players Games with Transformer Pattern
    
    Fetches games for each player in a specific month.
    
    Equivalent to: @dlt.transformer(data_from=players, write_disposition="append")
    def players_games(username): ...
    
    Args:
        title: Player title (GM, IM, FM, etc.)
        max_players: Maximum number of players
        year: Year for games
        month: Month for games (1-12)
    """
    import dlt
    import json
    import requests
    from dlt_snowpark import DltSnowparkLoader
    
    CHESS_URL = "https://api.chess.com/pub"
    HEADERS = {"User-Agent": "dlt-snowpark-chess-pipeline/1.0 (Snowflake SP)"}
    loader = DltSnowparkLoader(session, pipeline_name="chess_players_games")
    
    # Fetch titled players
    response = requests.get(f"{CHESS_URL}/titled/{title}", headers=HEADERS)
    response.raise_for_status()
    players_list = response.json()["players"][:max_players]
    
    # Fetch games for each player (transformer pattern)
    all_games = []
    players_with_games = 0
    
    for username in players_list:
        path = f"/player/{username}/games/{year:04d}/{month:02d}"
        try:
            response = requests.get(f"{CHESS_URL}{path}", headers=HEADERS)
            response.raise_for_status()
            games_data = response.json().get("games", [])
            
            for game in games_data:
                flat_game = {
                    "url": game.get("url"),
                    "pgn": game.get("pgn", "")[:1000] if game.get("pgn") else None,  # Truncate long PGN
                    "time_control": game.get("time_control"),
                    "end_time": game.get("end_time"),
                    "rated": game.get("rated"),
                    "time_class": game.get("time_class"),
                    "rules": game.get("rules"),
                    "white_username": game.get("white", {}).get("username"),
                    "white_rating": game.get("white", {}).get("rating"),
                    "white_result": game.get("white", {}).get("result"),
                    "black_username": game.get("black", {}).get("username"),
                    "black_rating": game.get("black", {}).get("rating"),
                    "black_result": game.get("black", {}).get("result"),
                    "fetched_for_player": username,
                    "game_year": year,
                    "game_month": month,
                }
                all_games.append(flat_game)
            
            if games_data:
                players_with_games += 1
                
        except requests.HTTPError as exc:
            # Allow players to not have games for some months (like the original example)
            if exc.response.status_code != 404:
                raise
    
    @dlt.resource(name="players_games", write_disposition="replace")
    def get_games():
        yield all_games if all_games else [{"no_games": True, "note": "No games found for the specified criteria"}]
    
    result = loader.run(get_games())
    verification = loader.verify_table("players_games")
    
    return json.dumps({
        "load_result": result,
        "verification": verification,
        "title": title,
        "year": year,
        "month": month,
        "players_checked": len(players_list),
        "players_with_games": players_with_games,
        "total_games": len(all_games)
    }, indent=2, default=str)
';

-- ============================================================
-- Procedure 4: P_CHESS_FULL_PIPELINE
-- Complete pipeline: players | profiles, players | games
-- This is the main "production" pipeline combining all resources
-- ============================================================
CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.CHESS_PRODUCTION."P_CHESS_FULL_PIPELINE"(
    "TITLE" VARCHAR DEFAULT 'GM',
    "MAX_PLAYERS" INTEGER DEFAULT 5,
    "YEAR" INTEGER DEFAULT 2024,
    "MONTH" INTEGER DEFAULT 12
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python','dlt','requests')
HANDLER = 'main'
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
EXTERNAL_ACCESS_INTEGRATIONS = (CHESS_API_ACCESS_INTEGRATION)
EXECUTE AS OWNER
AS '
def main(session, title: str, max_players: int, year: int, month: int):
    """
    Full Chess Production Pipeline
    
    Implements the complete chess_production example with all resources:
    1. players() - List of titled players
    2. players_profiles - Profile for each player (parallel fetch)
    3. players_games - Games for each player in specified month
    
    In dlt this would be:
        return players(), players_profiles, players_games
    
    Where players_profiles and players_games are transformers that depend on players.
    """
    import dlt
    import json
    import requests
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from dlt_snowpark import DltSnowparkLoader
    
    CHESS_URL = "https://api.chess.com/pub"
    HEADERS = {"User-Agent": "dlt-snowpark-chess-pipeline/1.0 (Snowflake SP)"}
    loader = DltSnowparkLoader(session, pipeline_name="chess_full_pipeline")
    
    # ============================================================
    # Step 1: Fetch players (this is the base players() resource)
    # ============================================================
    response = requests.get(f"{CHESS_URL}/titled/{title}", headers=HEADERS)
    response.raise_for_status()
    players_list = response.json()["players"][:max_players]
    
    # ============================================================
    # Step 2: Fetch profiles in parallel (players_profiles transformer)
    # ============================================================
    def fetch_profile(username):
        try:
            resp = requests.get(f"{CHESS_URL}/player/{username}", headers=HEADERS)
            resp.raise_for_status()
            return resp.json()
        except:
            return None
    
    profiles_list = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_profile, u): u for u in players_list}
        for future in as_completed(futures):
            result = future.result()
            if result:
                profiles_list.append(result)
    
    # ============================================================
    # Step 3: Fetch games for each player (players_games transformer)
    # ============================================================
    all_games = []
    for username in players_list:
        try:
            resp = requests.get(f"{CHESS_URL}/player/{username}/games/{year:04d}/{month:02d}", headers=HEADERS)
            resp.raise_for_status()
            games = resp.json().get("games", [])
            for game in games:
                game["_fetched_for"] = username
                all_games.append(game)
        except requests.HTTPError as e:
            if e.response.status_code != 404:
                raise
    
    # ============================================================
    # Resource 1: Players
    # ============================================================
    @dlt.resource(name="players", write_disposition="replace")
    def get_players():
        processed = []
        for idx, username in enumerate(players_list):
            processed.append({
                "username": username,
                "title": title,
                "list_position": idx + 1,
            })
        yield processed
    
    # ============================================================
    # Resource 2: Players Profiles
    # ============================================================
    @dlt.resource(name="players_profiles", write_disposition="replace")
    def get_profiles():
        flattened = []
        for profile in profiles_list:
            flat_profile = {
                "player_id": profile.get("player_id"),
                "username": profile.get("username"),
                "title": profile.get("title"),
                "name": profile.get("name"),
                "country": profile.get("country"),
                "location": profile.get("location"),
                "followers": profile.get("followers"),
                "joined": profile.get("joined"),
                "last_online": profile.get("last_online"),
                "status": profile.get("status"),
                "is_streamer": profile.get("is_streamer"),
                "verified": profile.get("verified"),
                "league": profile.get("league"),
                "avatar": profile.get("avatar"),
                "url": profile.get("url"),
            }
            flattened.append(flat_profile)
        yield flattened
    
    # ============================================================
    # Resource 3: Players Games
    # ============================================================
    @dlt.resource(name="players_games", write_disposition="replace")
    def get_games():
        if not all_games:
            yield [{"no_games": True}]
            return
            
        flattened = []
        for game in all_games:
            flat_game = {
                "url": game.get("url"),
                "time_control": game.get("time_control"),
                "end_time": game.get("end_time"),
                "rated": game.get("rated"),
                "time_class": game.get("time_class"),
                "rules": game.get("rules"),
                "white_username": game.get("white", {}).get("username"),
                "white_rating": game.get("white", {}).get("rating"),
                "white_result": game.get("white", {}).get("result"),
                "black_username": game.get("black", {}).get("username"),
                "black_rating": game.get("black", {}).get("rating"),
                "black_result": game.get("black", {}).get("result"),
                "fetched_for_player": game.get("_fetched_for"),
                "game_year": year,
                "game_month": month,
            }
            flattened.append(flat_game)
        yield flattened
    
    # Run all resources in single pipeline run
    result = loader.run(get_players(), get_profiles(), get_games())
    
    # Verify all tables
    verifications = {
        "players": loader.verify_table("players"),
        "players_profiles": loader.verify_table("players_profiles"),
        "players_games": loader.verify_table("players_games"),
    }
    
    return json.dumps({
        "load_result": result,
        "verifications": verifications,
        "summary": {
            "title": title,
            "max_players": max_players,
            "year": year,
            "month": month,
            "players_loaded": len(players_list),
            "profiles_loaded": len(profiles_list),
            "games_loaded": len(all_games),
        },
        "note": "Full chess production pipeline with players, profiles, and games"
    }, indent=2, default=str)
';

-- ============================================================
-- Procedure 5: P_CHESS_MULTI_TITLE
-- Dynamic resources: Fetch data for multiple player titles
-- Demonstrates dynamic resource creation pattern
-- ============================================================
CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.CHESS_PRODUCTION."P_CHESS_MULTI_TITLE"(
    "MAX_PLAYERS_PER_TITLE" INTEGER DEFAULT 3
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python','dlt','requests')
HANDLER = 'main'
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
EXTERNAL_ACCESS_INTEGRATIONS = (CHESS_API_ACCESS_INTEGRATION)
EXECUTE AS OWNER
AS '
def main(session, max_players_per_title: int):
    """
    Multi-Title Chess Players
    
    Fetches players and profiles across multiple title categories.
    Demonstrates the dynamic resource pattern similar to the GitHub dynamic resources.
    
    Titles: GM (Grandmaster), IM (International Master), FM (FIDE Master), WGM (Woman Grandmaster)
    """
    import json
    import requests
    import dlt
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from dlt_snowpark import DltSnowparkLoader
    
    CHESS_URL = "https://api.chess.com/pub"
    HEADERS = {"User-Agent": "dlt-snowpark-chess-pipeline/1.0 (Snowflake SP)"}
    TITLES = ["GM", "IM", "FM", "WGM"]
    
    loader = DltSnowparkLoader(session, pipeline_name="chess_multi_title")
    
    def fetch_profile(username):
        try:
            resp = requests.get(f"{CHESS_URL}/player/{username}", headers=HEADERS)
            resp.raise_for_status()
            return resp.json()
        except:
            return None
    
    # Fetch players and profiles for each title
    all_data = {}
    for title in TITLES:
        try:
            response = requests.get(f"{CHESS_URL}/titled/{title}", headers=HEADERS)
            response.raise_for_status()
            players = response.json()["players"][:max_players_per_title]
            
            # Fetch profiles in parallel
            profiles = []
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = {executor.submit(fetch_profile, u): u for u in players}
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        profiles.append(result)
            
            all_data[title] = {
                "players": players,
                "profiles": profiles
            }
        except:
            all_data[title] = {"players": [], "profiles": []}
    
    # Create resources
    @dlt.resource(name="titled_players", write_disposition="replace")
    def get_all_players():
        all_players = []
        for title, data in all_data.items():
            for idx, username in enumerate(data["players"]):
                all_players.append({
                    "username": username,
                    "title": title,
                    "title_rank": TITLES.index(title) + 1,
                    "position_in_title": idx + 1,
                })
        yield all_players
    
    @dlt.resource(name="titled_profiles", write_disposition="replace")
    def get_all_profiles():
        all_profiles = []
        for title, data in all_data.items():
            for profile in data["profiles"]:
                flat_profile = {
                    "player_id": profile.get("player_id"),
                    "username": profile.get("username"),
                    "title": profile.get("title"),
                    "name": profile.get("name"),
                    "country": profile.get("country"),
                    "followers": profile.get("followers"),
                    "joined": profile.get("joined"),
                    "last_online": profile.get("last_online"),
                    "status": profile.get("status"),
                    "is_streamer": profile.get("is_streamer"),
                    "url": profile.get("url"),
                    "title_category": title,  # Track which category
                }
                all_profiles.append(flat_profile)
        yield all_profiles
    
    result = loader.run(get_all_players(), get_all_profiles())
    
    # Summary by title
    summary = {}
    for title in TITLES:
        summary[title] = {
            "players": len(all_data[title]["players"]),
            "profiles": len(all_data[title]["profiles"])
        }
    
    return json.dumps({
        "load_result": result,
        "verifications": {
            "titled_players": loader.verify_table("titled_players"),
            "titled_profiles": loader.verify_table("titled_profiles"),
        },
        "summary_by_title": summary,
        "titles_fetched": TITLES,
        "note": "Multi-title dynamic resource pattern"
    }, indent=2, default=str)
';

-- ============================================================
-- Procedure 6: P_CHESS_STATS
-- Player statistics and ratings
-- Demonstrates fetching additional endpoints
-- ============================================================
CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.CHESS_PRODUCTION."P_CHESS_STATS"(
    "TITLE" VARCHAR DEFAULT 'GM',
    "MAX_PLAYERS" INTEGER DEFAULT 5
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python','dlt','requests')
HANDLER = 'main'
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
EXTERNAL_ACCESS_INTEGRATIONS = (CHESS_API_ACCESS_INTEGRATION)
EXECUTE AS OWNER
AS '
def main(session, title: str, max_players: int):
    """
    Chess Player Statistics
    
    Fetches player statistics including ratings across different time controls.
    Uses the /player/{username}/stats endpoint.
    """
    import dlt
    import json
    import requests
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from dlt_snowpark import DltSnowparkLoader
    
    CHESS_URL = "https://api.chess.com/pub"
    HEADERS = {"User-Agent": "dlt-snowpark-chess-pipeline/1.0 (Snowflake SP)"}
    loader = DltSnowparkLoader(session, pipeline_name="chess_stats")
    
    # Fetch titled players
    response = requests.get(f"{CHESS_URL}/titled/{title}", headers=HEADERS)
    response.raise_for_status()
    players_list = response.json()["players"][:max_players]
    
    def fetch_stats(username):
        try:
            resp = requests.get(f"{CHESS_URL}/player/{username}/stats", headers=HEADERS)
            resp.raise_for_status()
            data = resp.json()
            data["username"] = username
            return data
        except:
            return None
    
    # Fetch stats in parallel
    stats_list = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_stats, u): u for u in players_list}
        for future in as_completed(futures):
            result = future.result()
            if result:
                stats_list.append(result)
    
    @dlt.resource(name="player_stats", write_disposition="replace")
    def get_stats():
        flattened = []
        for stats in stats_list:
            # Extract ratings for different time controls
            def get_rating(category):
                cat_data = stats.get(category, {})
                last = cat_data.get("last", {})
                best = cat_data.get("best", {})
                record = cat_data.get("record", {})
                return {
                    f"{category}_rating": last.get("rating"),
                    f"{category}_best": best.get("rating"),
                    f"{category}_wins": record.get("win", 0),
                    f"{category}_losses": record.get("loss", 0),
                    f"{category}_draws": record.get("draw", 0),
                }
            
            flat_stats = {
                "username": stats.get("username"),
                "fide": stats.get("fide"),
            }
            
            # Add ratings for each time control
            for category in ["chess_rapid", "chess_blitz", "chess_bullet", "chess_daily"]:
                flat_stats.update(get_rating(category))
            
            # Add puzzle and tactics ratings
            puzzle_rush = stats.get("puzzle_rush", {}).get("best", {})
            flat_stats["puzzle_rush_best"] = puzzle_rush.get("score")
            
            tactics = stats.get("tactics", {}).get("highest", {})
            flat_stats["tactics_highest"] = tactics.get("rating")
            
            flattened.append(flat_stats)
        yield flattened
    
    result = loader.run(get_stats())
    verification = loader.verify_table("player_stats")
    
    return json.dumps({
        "load_result": result,
        "verification": verification,
        "title": title,
        "stats_fetched": len(stats_list),
        "note": "Player statistics with ratings across time controls"
    }, indent=2, default=str)
';

