-- Transformers Tutorial Stored Procedures
-- Database: DLT_SNOWPARK_SFRT
-- Schema: TRANSFORMERS_TUTORIAL
-- These procedures demonstrate dlt transformer patterns with PokeAPI
-- Based on: https://github.com/dlt-hub/dlt/tree/master/docs/examples/transformers

-- Prerequisites (run once):
-- CREATE NETWORK RULE DLT_SNOWPARK_SFRT.TRANSFORMERS_TUTORIAL.POKEAPI_NETWORK_RULE
--   MODE = EGRESS TYPE = HOST_PORT VALUE_LIST = ('pokeapi.co:443');
-- CREATE EXTERNAL ACCESS INTEGRATION POKEAPI_ACCESS_INTEGRATION
--   ALLOWED_NETWORK_RULES = (DLT_SNOWPARK_SFRT.TRANSFORMERS_TUTORIAL.POKEAPI_NETWORK_RULE)
--   ENABLED = TRUE;

CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.TRANSFORMERS_TUTORIAL."P_POKEMON_LIST_BASIC"()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python','dlt','requests')
HANDLER = 'main'
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
EXTERNAL_ACCESS_INTEGRATIONS = (POKEAPI_ACCESS_INTEGRATION)
EXECUTE AS OWNER
AS '
def main(session):
    """
    Basic Pokemon List Resource

    Fetches the first page of Pokemon from PokeAPI.
    This is the foundation before using transformers.
    """
    import json
    import requests
    from dlt_snowpark import DltSnowparkLoader

    loader = DltSnowparkLoader(session, pipeline_name="pokemon_basic")

    # Fetch Pokemon list from PokeAPI
    response = requests.get("https://pokeapi.co/api/v2/pokemon", params={"limit": 20})
    response.raise_for_status()
    pokemon_list = response.json()["results"]

    @loader.resource(name="pokemon_list", write_disposition="replace")
    def get_pokemon_list():
        """Yields the list of Pokemon with basic info"""
        processed = []
        for idx, pokemon in enumerate(pokemon_list):
            processed.append({
                "name": pokemon["name"],
                "url": pokemon["url"],
                "list_position": idx + 1,
            })
        yield processed

    result = loader.run(get_pokemon_list())
    verification = loader.verify_table("pokemon_list")

    return json.dumps({
        "load_result": result,
        "verification": verification,
        "note": "Basic Pokemon list without transformers"
    }, indent=2, default=str)
';

CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.TRANSFORMERS_TUTORIAL."P_POKEMON_WITH_DETAILS"()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python','dlt','requests')
HANDLER = 'main'
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
EXTERNAL_ACCESS_INTEGRATIONS = (POKEAPI_ACCESS_INTEGRATION)
EXECUTE AS OWNER
AS '
def main(session):
    """
    Pokemon Details with Transformer Pattern

    Implements the dlt transformer pattern:
    - pokemon_list: Base resource that yields Pokemon names/URLs (not loaded)
    - pokemon: Transformer that fetches details for each Pokemon

    Equivalent to: pokemon_list | pokemon (from the dlt example)
    """
    import json
    import requests
    from dlt_snowpark import DltSnowparkLoader

    loader = DltSnowparkLoader(session, pipeline_name="pokemon_details")

    # Fetch Pokemon list (this would be pokemon_list resource with selected=False)
    list_response = requests.get("https://pokeapi.co/api/v2/pokemon", params={"limit": 10})
    list_response.raise_for_status()
    pokemon_list = list_response.json()["results"]

    def get_pokemon_details(pokemon_url):
        """Fetch detailed Pokemon information (equivalent to @dlt.transformer)"""
        response = requests.get(pokemon_url)
        response.raise_for_status()
        return response.json()

    @loader.resource(name="pokemon", write_disposition="replace")
    def pokemon_transformer():
        """
        Transformer that yields Pokemon details.

        In the dlt example, this would be:
        @dlt.transformer
        def pokemon(pokemons):
            for _pokemon in pokemons:
                yield _get_pokemon(_pokemon)
        """
        all_details = []
        for pokemon in pokemon_list:
            details = get_pokemon_details(pokemon["url"])
            # Flatten the complex Pokemon object
            flat_pokemon = {
                "id": details.get("id"),
                "name": details.get("name"),
                "height": details.get("height"),
                "weight": details.get("weight"),
                "base_experience": details.get("base_experience"),
                "order": details.get("order"),
                "is_default": details.get("is_default"),
                # Extract first type
                "type_1": details.get("types", [{}])[0].get("type", {}).get("name") if details.get("types") else None,
                # Extract second type if exists
                "type_2": details.get("types", [{}])[1].get("type", {}).get("name") if len(details.get("types", [])) > 1 else None,
                # Extract abilities as comma-separated string
                "abilities": ", ".join([a.get("ability", {}).get("name", "") for a in details.get("abilities", [])]),
                # Stats
                "hp": next((s["base_stat"] for s in details.get("stats", []) if s["stat"]["name"] == "hp"), None),
                "attack": next((s["base_stat"] for s in details.get("stats", []) if s["stat"]["name"] == "attack"), None),
                "defense": next((s["base_stat"] for s in details.get("stats", []) if s["stat"]["name"] == "defense"), None),
                "speed": next((s["base_stat"] for s in details.get("stats", []) if s["stat"]["name"] == "speed"), None),
                # Species URL for next transformer
                "species_url": details.get("species", {}).get("url"),
                "sprite_url": details.get("sprites", {}).get("front_default"),
            }
            all_details.append(flat_pokemon)
        yield all_details

    result = loader.run(pokemon_transformer())
    verification = loader.verify_table("pokemon")

    return json.dumps({
        "load_result": result,
        "verification": verification,
        "note": "Pokemon details loaded via transformer pattern"
    }, indent=2, default=str)
';

CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.TRANSFORMERS_TUTORIAL."P_POKEMON_FULL_TRANSFORMER_CHAIN"()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python','dlt','requests')
HANDLER = 'main'
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
EXTERNAL_ACCESS_INTEGRATIONS = (POKEAPI_ACCESS_INTEGRATION)
EXECUTE AS OWNER
AS '
def main(session):
    """
    Full Transformer Chain: pokemon_list | pokemon | species

    Implements the complete dlt transformers example:
    1. pokemon_list: Get list of Pokemon (selected=False - not loaded)
    2. pokemon: Transform to get Pokemon details  
    3. species: Transform Pokemon details to get species info

    The pipe operator creates two pipelines:
    - pokemon_list | pokemon (loads Pokemon table)
    - pokemon_list | pokemon | species (loads species table)

    Note: dlt is smart enough to fetch pokemon_list and pokemon details only once!
    """
    import json
    import requests
    from dlt_snowpark import DltSnowparkLoader

    loader = DltSnowparkLoader(session, pipeline_name="pokemon_transformers")

    # ============================================================
    # Step 1: Fetch Pokemon list (would be pokemon_list resource)
    # ============================================================
    list_response = requests.get("https://pokeapi.co/api/v2/pokemon", params={"limit": 10})
    list_response.raise_for_status()
    pokemon_list = list_response.json()["results"]

    # ============================================================
    # Step 2: Fetch Pokemon details for each (pokemon transformer)
    # In dlt this would use @dlt.defer for parallel execution
    # ============================================================
    pokemon_details_list = []
    for pokemon in pokemon_list:
        response = requests.get(pokemon["url"])
        response.raise_for_status()
        pokemon_details_list.append(response.json())

    # ============================================================
    # Step 3: Fetch species details for each Pokemon (species transformer)
    # In dlt: @dlt.transformer(parallelized=True)
    # ============================================================
    species_details_list = []
    for pokemon_details in pokemon_details_list:
        species_url = pokemon_details.get("species", {}).get("url")
        if species_url:
            response = requests.get(species_url)
            response.raise_for_status()
            species_data = response.json()
            # Link back to Pokemon (as in the dlt example)
            species_data["pokemon_id"] = pokemon_details["id"]
            species_details_list.append(species_data)

    # ============================================================
    # Resource 1: Pokemon Details (from pokemon_list | pokemon)
    # ============================================================
    @loader.resource(name="pokemon", write_disposition="replace")
    def get_pokemon():
        """Yields flattened Pokemon details"""
        flattened = []
        for details in pokemon_details_list:
            flat_pokemon = {
                "id": details.get("id"),
                "name": details.get("name"),
                "height": details.get("height"),
                "weight": details.get("weight"),
                "base_experience": details.get("base_experience"),
                "order": details.get("order"),
                "type_1": details.get("types", [{}])[0].get("type", {}).get("name") if details.get("types") else None,
                "type_2": details.get("types", [{}])[1].get("type", {}).get("name") if len(details.get("types", [])) > 1 else None,
                "abilities": ", ".join([a.get("ability", {}).get("name", "") for a in details.get("abilities", [])]),
                "hp": next((s["base_stat"] for s in details.get("stats", []) if s["stat"]["name"] == "hp"), None),
                "attack": next((s["base_stat"] for s in details.get("stats", []) if s["stat"]["name"] == "attack"), None),
                "defense": next((s["base_stat"] for s in details.get("stats", []) if s["stat"]["name"] == "defense"), None),
                "speed": next((s["base_stat"] for s in details.get("stats", []) if s["stat"]["name"] == "speed"), None),
                "sprite_url": details.get("sprites", {}).get("front_default"),
            }
            flattened.append(flat_pokemon)
        yield flattened

    # ============================================================
    # Resource 2: Species Details (from pokemon_list | pokemon | species)
    # ============================================================
    @loader.resource(name="species", write_disposition="replace")
    def get_species():
        """
        Yields species details linked to Pokemon.

        In dlt example:
        @dlt.transformer(parallelized=True)
        def species(pokemon_details):
            species_data = requests.get(pokemon_details["species"]["url"]).json()
            species_data["pokemon_id"] = pokemon_details["id"]
            return species_data
        """
        flattened = []
        for species in species_details_list:
            # Get English flavor text
            flavor_text = None
            for entry in species.get("flavor_text_entries", []):
                if entry.get("language", {}).get("name") == "en":
                    flavor_text = entry.get("flavor_text", "").replace("\\n", " ").replace("\\f", " ")
                    break

            # Get English genus
            genus = None
            for gen in species.get("genera", []):
                if gen.get("language", {}).get("name") == "en":
                    genus = gen.get("genus")
                    break

            flat_species = {
                "id": species.get("id"),
                "pokemon_id": species.get("pokemon_id"),  # Link to Pokemon table
                "name": species.get("name"),
                "base_happiness": species.get("base_happiness"),
                "capture_rate": species.get("capture_rate"),
                "color": species.get("color", {}).get("name"),
                "egg_groups": ", ".join([eg.get("name", "") for eg in species.get("egg_groups", [])]),
                "generation": species.get("generation", {}).get("name"),
                "growth_rate": species.get("growth_rate", {}).get("name"),
                "habitat": species.get("habitat", {}).get("name") if species.get("habitat") else None,
                "has_gender_differences": species.get("has_gender_differences"),
                "is_baby": species.get("is_baby"),
                "is_legendary": species.get("is_legendary"),
                "is_mythical": species.get("is_mythical"),
                "genus": genus,
                "flavor_text": flavor_text[:500] if flavor_text else None,  # Truncate long text
            }
            flattened.append(flat_species)
        yield flattened

    # Run both resources (equivalent to the dlt example''s return statement)
    # In dlt: return (pokemon_list | pokemon, pokemon_list | pokemon | species)
    result = loader.run(get_pokemon(), get_species())

    pokemon_verification = loader.verify_table("pokemon")
    species_verification = loader.verify_table("species")

    return json.dumps({
        "load_result": result,
        "verifications": {
            "pokemon": pokemon_verification,
            "species": species_verification
        },
        "note": "Full transformer chain: pokemon_list | pokemon | species"
    }, indent=2, default=str)
';

CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.TRANSFORMERS_TUTORIAL."P_POKEMON_PARALLEL_FETCH"()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python','dlt','requests')
HANDLER = 'main'
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
EXTERNAL_ACCESS_INTEGRATIONS = (POKEAPI_ACCESS_INTEGRATION)
EXECUTE AS OWNER
AS '
def main(session):
    """
    Parallel Fetch Pattern with ThreadPoolExecutor

    Demonstrates parallel data fetching similar to dlt''s @dlt.defer decorator.

    In the dlt example:
    @dlt.transformer
    def pokemon(pokemons):
        @dlt.defer
        def _get_pokemon(_pokemon):
            return requests.get(_pokemon["url"]).json()

        for _pokemon in pokemons:
            yield _get_pokemon(_pokemon)

    Here we use ThreadPoolExecutor for parallel HTTP calls.
    """
    import json
    import requests
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from dlt_snowpark import DltSnowparkLoader

    loader = DltSnowparkLoader(session, pipeline_name="pokemon_parallel")

    # Fetch Pokemon list
    list_response = requests.get("https://pokeapi.co/api/v2/pokemon", params={"limit": 20})
    list_response.raise_for_status()
    pokemon_list = list_response.json()["results"]

    def fetch_pokemon_details(pokemon):
        """Fetch Pokemon details - equivalent to @dlt.defer function"""
        response = requests.get(pokemon["url"])
        response.raise_for_status()
        return response.json()

    # Use ThreadPoolExecutor for parallel fetching (like @dlt.defer)
    pokemon_details_list = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_pokemon = {
            executor.submit(fetch_pokemon_details, pokemon): pokemon 
            for pokemon in pokemon_list
        }
        for future in as_completed(future_to_pokemon):
            try:
                details = future.result()
                pokemon_details_list.append(details)
            except Exception as e:
                # Log error but continue
                pass

    # Sort by ID to maintain consistent order
    pokemon_details_list.sort(key=lambda x: x.get("id", 0))

    @loader.resource(name="pokemon_parallel", write_disposition="replace")
    def get_pokemon():
        """Yields Pokemon details fetched in parallel"""
        flattened = []
        for details in pokemon_details_list:
            flat_pokemon = {
                "id": details.get("id"),
                "name": details.get("name"),
                "height": details.get("height"),
                "weight": details.get("weight"),
                "base_experience": details.get("base_experience"),
                "type_1": details.get("types", [{}])[0].get("type", {}).get("name") if details.get("types") else None,
                "type_2": details.get("types", [{}])[1].get("type", {}).get("name") if len(details.get("types", [])) > 1 else None,
                "abilities": ", ".join([a.get("ability", {}).get("name", "") for a in details.get("abilities", [])]),
                "hp": next((s["base_stat"] for s in details.get("stats", []) if s["stat"]["name"] == "hp"), None),
                "attack": next((s["base_stat"] for s in details.get("stats", []) if s["stat"]["name"] == "attack"), None),
                "defense": next((s["base_stat"] for s in details.get("stats", []) if s["stat"]["name"] == "defense"), None),
                "speed": next((s["base_stat"] for s in details.get("stats", []) if s["stat"]["name"] == "speed"), None),
            }
            flattened.append(flat_pokemon)
        yield flattened

    result = loader.run(get_pokemon())
    verification = loader.verify_table("pokemon_parallel")

    return json.dumps({
        "load_result": result,
        "verification": verification,
        "pokemon_count": len(pokemon_details_list),
        "note": "Pokemon fetched in parallel using ThreadPoolExecutor (like @dlt.defer)"
    }, indent=2, default=str)
';

CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.TRANSFORMERS_TUTORIAL."P_POKEMON_DYNAMIC_TRANSFORMERS"()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python','dlt','requests')
HANDLER = 'main'
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
EXTERNAL_ACCESS_INTEGRATIONS = (POKEAPI_ACCESS_INTEGRATION)
EXECUTE AS OWNER
AS '
def main(session):
    """
    Dynamic Transformers for Multiple PokeAPI Endpoints

    Creates transformers dynamically for different API endpoints:
    - Pokemon (creatures)
    - Types (fire, water, grass, etc.)
    - Abilities (overgrow, blaze, torrent, etc.)

    Each endpoint has its own transformer logic.
    """
    import json
    import requests
    from dlt_snowpark import DltSnowparkLoader

    loader = DltSnowparkLoader(session, pipeline_name="pokemon_dynamic")

    # Define endpoints with their configurations
    ENDPOINTS = [
        {
            "name": "types",
            "url": "https://pokeapi.co/api/v2/type",
            "limit": 18,  # 18 base types
            "detail_fields": ["id", "name", "damage_relations"],
        },
        {
            "name": "abilities",
            "url": "https://pokeapi.co/api/v2/ability",
            "limit": 20,
            "detail_fields": ["id", "name", "effect_entries", "pokemon"],
        },
        {
            "name": "berries",
            "url": "https://pokeapi.co/api/v2/berry",
            "limit": 10,
            "detail_fields": ["id", "name", "firmness", "size", "growth_time"],
        },
    ]

    def fetch_endpoint_list(url, limit):
        """Fetch list from endpoint"""
        response = requests.get(url, params={"limit": limit})
        response.raise_for_status()
        return response.json()["results"]

    def fetch_details(url):
        """Fetch details for a single item"""
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def flatten_type(details):
        """Flatten type details"""
        # Get damage relations
        dr = details.get("damage_relations", {})
        return {
            "id": details.get("id"),
            "name": details.get("name"),
            "double_damage_from": ", ".join([t["name"] for t in dr.get("double_damage_from", [])]),
            "double_damage_to": ", ".join([t["name"] for t in dr.get("double_damage_to", [])]),
            "half_damage_from": ", ".join([t["name"] for t in dr.get("half_damage_from", [])]),
            "half_damage_to": ", ".join([t["name"] for t in dr.get("half_damage_to", [])]),
            "no_damage_from": ", ".join([t["name"] for t in dr.get("no_damage_from", [])]),
            "no_damage_to": ", ".join([t["name"] for t in dr.get("no_damage_to", [])]),
            "pokemon_count": len(details.get("pokemon", [])),
        }

    def flatten_ability(details):
        """Flatten ability details"""
        # Get English effect
        effect = None
        for entry in details.get("effect_entries", []):
            if entry.get("language", {}).get("name") == "en":
                effect = entry.get("effect", "")[:500]
                break
        return {
            "id": details.get("id"),
            "name": details.get("name"),
            "is_main_series": details.get("is_main_series"),
            "generation": details.get("generation", {}).get("name"),
            "effect": effect,
            "pokemon_count": len(details.get("pokemon", [])),
        }

    def flatten_berry(details):
        """Flatten berry details"""
        return {
            "id": details.get("id"),
            "name": details.get("name"),
            "firmness": details.get("firmness", {}).get("name"),
            "size": details.get("size"),
            "growth_time": details.get("growth_time"),
            "max_harvest": details.get("max_harvest"),
            "natural_gift_power": details.get("natural_gift_power"),
            "natural_gift_type": details.get("natural_gift_type", {}).get("name") if details.get("natural_gift_type") else None,
            "smoothness": details.get("smoothness"),
            "soil_dryness": details.get("soil_dryness"),
        }

    FLATTENERS = {
        "types": flatten_type,
        "abilities": flatten_ability,
        "berries": flatten_berry,
    }

    # Fetch all data
    all_data = {}
    for endpoint in ENDPOINTS:
        items = fetch_endpoint_list(endpoint["url"], endpoint["limit"])
        details_list = [fetch_details(item["url"]) for item in items]
        flattener = FLATTENERS[endpoint["name"]]
        all_data[endpoint["name"]] = [flattener(d) for d in details_list]

    # Create resources
    @loader.resource(name="poke_types", write_disposition="replace")
    def get_types():
        yield all_data["types"]

    @loader.resource(name="poke_abilities", write_disposition="replace")
    def get_abilities():
        yield all_data["abilities"]

    @loader.resource(name="poke_berries", write_disposition="replace")
    def get_berries():
        yield all_data["berries"]

    result = loader.run(get_types(), get_abilities(), get_berries())

    verifications = {
        "types": loader.verify_table("poke_types"),
        "abilities": loader.verify_table("poke_abilities"),
        "berries": loader.verify_table("poke_berries"),
    }

    return json.dumps({
        "load_result": result,
        "verifications": verifications,
        "note": "Dynamic transformers for multiple PokeAPI endpoints"
    }, indent=2, default=str)
';

CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.TRANSFORMERS_TUTORIAL."P_POKEMON_ADD_MAP_PATTERN"()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python','dlt','requests')
HANDLER = 'main'
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
EXTERNAL_ACCESS_INTEGRATIONS = (POKEAPI_ACCESS_INTEGRATION)
EXECUTE AS OWNER
AS '
def main(session):
    """
    Add Map Pattern - Transform Each Item

    Demonstrates item-level transformations similar to dlt''s add_map:

    resource.add_map(transform_function)

    In this example:
    1. Fetch Pokemon
    2. Apply transformations: calculate BMI, categorize by generation, etc.
    """
    import json
    import requests
    from dlt_snowpark import DltSnowparkLoader

    loader = DltSnowparkLoader(session, pipeline_name="pokemon_add_map")

    # Fetch Pokemon list with details
    list_response = requests.get("https://pokeapi.co/api/v2/pokemon", params={"limit": 15})
    list_response.raise_for_status()
    pokemon_list = list_response.json()["results"]

    pokemon_details = []
    for pokemon in pokemon_list:
        response = requests.get(pokemon["url"])
        response.raise_for_status()
        pokemon_details.append(response.json())

    # Transform functions (like add_map)
    def calculate_bmi(pokemon):
        """Calculate Pokemon BMI (weight/height^2)"""
        height_m = pokemon.get("height", 1) / 10  # height is in decimeters
        weight_kg = pokemon.get("weight", 1) / 10  # weight is in hectograms
        if height_m > 0:
            return round(weight_kg / (height_m ** 2), 2)
        return None

    def get_power_tier(pokemon):
        """Categorize Pokemon by total base stats"""
        total_stats = sum(s.get("base_stat", 0) for s in pokemon.get("stats", []))
        if total_stats >= 500:
            return "Legendary"
        elif total_stats >= 400:
            return "Strong"
        elif total_stats >= 300:
            return "Average"
        else:
            return "Weak"

    def get_size_category(pokemon):
        """Categorize Pokemon by height"""
        height = pokemon.get("height", 0) / 10  # meters
        if height >= 2:
            return "Large"
        elif height >= 1:
            return "Medium"
        else:
            return "Small"

    @loader.resource(name="pokemon_enriched", write_disposition="replace")
    def get_enriched_pokemon():
        """Yields Pokemon with additional computed fields (add_map pattern)"""
        enriched = []
        for details in pokemon_details:
            # Base data
            pokemon = {
                "id": details.get("id"),
                "name": details.get("name"),
                "height": details.get("height"),
                "weight": details.get("weight"),
                "base_experience": details.get("base_experience"),
                "type_1": details.get("types", [{}])[0].get("type", {}).get("name") if details.get("types") else None,
                "hp": next((s["base_stat"] for s in details.get("stats", []) if s["stat"]["name"] == "hp"), None),
                "attack": next((s["base_stat"] for s in details.get("stats", []) if s["stat"]["name"] == "attack"), None),
                "defense": next((s["base_stat"] for s in details.get("stats", []) if s["stat"]["name"] == "defense"), None),
                "speed": next((s["base_stat"] for s in details.get("stats", []) if s["stat"]["name"] == "speed"), None),
                "total_stats": sum(s.get("base_stat", 0) for s in details.get("stats", [])),
            }

            # Apply transformations (like add_map)
            pokemon["bmi"] = calculate_bmi(details)
            pokemon["power_tier"] = get_power_tier(details)
            pokemon["size_category"] = get_size_category(details)

            enriched.append(pokemon)
        yield enriched

    result = loader.run(get_enriched_pokemon())
    verification = loader.verify_table("pokemon_enriched")

    return json.dumps({
        "load_result": result,
        "verification": verification,
        "note": "Pokemon with computed fields (add_map pattern)"
    }, indent=2, default=str)
';
