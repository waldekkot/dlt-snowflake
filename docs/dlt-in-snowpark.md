# Running dlt Inside Snowflake Stored Procedures

## Overview

This document captures the findings from attempting to run [dlt (data load tool)](https://dlthub.com/) inside Snowflake Python stored procedures. The goal was to use dlt's powerful ETL capabilities directly within Snowflake's compute environment.

## TL;DR

**dlt's built-in Snowflake destination does NOT work inside stored procedures** due to Snowflake limitations on the `PUT` command. The solution is to use **dlt's custom destination** with Snowpark's native `save_as_table()` method.

A reusable helper module (`dlt_snowpark.py`) is provided that encapsulates this workaround for easy use across multiple stored procedures.

### Key Findings

| Finding | Details |
|---------|---------|
| **PUT command blocked** | Snowflake SPs cannot execute `PUT` command (used by dlt internally) |
| **Pandas NOT needed** | Snowpark's `create_dataframe()` accepts lists of dicts directly |
| **External APIs work** | With EXTERNAL_ACCESS_INTEGRATIONS, SPs can call external APIs (GitHub, PokeAPI, Chess.com) |
| **Full ETL support** | Read from Snowflake → Transform → Write back to Snowflake |
| **dlt patterns work** | Transformers, parallel fetch, dynamic resources, add_map all validated |

---

## Database Structure

The recommended structure separates the reusable module from tutorial/application code:

```
DLT_SNOWPARK_SFRT/                    # Database
├── DLT_SNOWPARK/                     # Schema: Shared modules
│   └── MODULES (stage)               # Contains dlt_snowpark.py
├── SIMPLE_DEMO/                      # Schema: Simple examples (no external API)
│   ├── Stored Procedures (7)
│   └── Tables (6)
├── COMPLEX_DEMO/                     # Schema: Complex validation pipelines
│   ├── Stored Procedures (6)
│   └── Tables (24)
├── GITHUB_TUTORIAL/                  # Schema: GitHub API tutorial
│   ├── Stored Procedures (6)
│   ├── Tables (10)
│   └── Network Rule + EAI (api.github.com)
├── TRANSFORMERS_TUTORIAL/            # Schema: Pokemon transformers tutorial
│   ├── Stored Procedures (6)
│   ├── Tables (6)
│   └── Network Rule + EAI (pokeapi.co)
└── CHESS_PRODUCTION/                 # Schema: Chess.com production pipeline
    ├── Stored Procedures (6)
    ├── Tables (6)
    └── Network Rule + EAI (api.chess.com)
```

| Schema | Purpose |
|--------|---------|
| `DLT_SNOWPARK` | Shared helper module storage |
| `SIMPLE_DEMO` | Basic examples without external API dependencies |
| `COMPLEX_DEMO` | Complex validation pipelines with large datasets |
| `GITHUB_TUTORIAL` | GitHub API tutorial procedures and data |
| `TRANSFORMERS_TUTORIAL` | Pokemon transformers tutorial procedures and data |
| `CHESS_PRODUCTION` | Chess.com production pipeline tutorial |

---

## Quick Start

### 1. Create Database and Schemas

```sql
CREATE DATABASE IF NOT EXISTS DLT_SNOWPARK_SFRT;
CREATE SCHEMA IF NOT EXISTS DLT_SNOWPARK_SFRT.DLT_SNOWPARK;
CREATE STAGE IF NOT EXISTS DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES;
```

### 2. Upload the Helper Module

Using Snowflake CLI:

```bash
snow stage copy dlt_snowpark.py @DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/ -c dlt-demo --overwrite
```

Or using SQL:

```sql
PUT file:///path/to/dlt_snowpark.py @DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
```

### 3. Create a Stored Procedure

```sql
CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.MY_SCHEMA.P_MY_PIPELINE()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'main'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python', 'dlt')
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
AS
$$
def main(session):
    import dlt
    from dlt_snowpark import DltSnowparkLoader
    
    loader = DltSnowparkLoader(session, pipeline_name="my_pipeline")
    
    @dlt.resource(name="my_table", write_disposition="replace")
    def get_data():
        yield [
            {"id": 1, "name": "Item 1"},
            {"id": 2, "name": "Item 2"},
        ]
    
    return loader.run_json(get_data())
$$;
```

### 4. Call It

```sql
CALL DLT_SNOWPARK_SFRT.MY_SCHEMA.P_MY_PIPELINE();
```

---

## Simple Demo Examples

The Simple Demo (`examples/simple_demo_stored_procedures.sql`) provides basic examples that demonstrate core dlt patterns without requiring external API access.

### Deploy Procedures

```bash
snow sql -c dlt-demo -q "CREATE SCHEMA IF NOT EXISTS DLT_SNOWPARK_SFRT.SIMPLE_DEMO;"
snow sql -c dlt-demo -f examples/simple_demo_stored_procedures.sql
```

### Run and Validate

```sql
-- Check dlt version and context
CALL DLT_SNOWPARK_SFRT.SIMPLE_DEMO.P_DLT_VERSION();
CALL DLT_SNOWPARK_SFRT.SIMPLE_DEMO.P_CHECK_CONTEXT();

-- Basic data loading
CALL DLT_SNOWPARK_SFRT.SIMPLE_DEMO.P_BASIC_DATA_LOAD();

-- Multiple tables in one pipeline
CALL DLT_SNOWPARK_SFRT.SIMPLE_DEMO.P_MULTIPLE_TABLES();

-- Complex data structures
CALL DLT_SNOWPARK_SFRT.SIMPLE_DEMO.P_NESTED_JSON();

-- Append mode (run multiple times to see accumulation)
CALL DLT_SNOWPARK_SFRT.SIMPLE_DEMO.P_APPEND_MODE();

-- Generator pattern for larger datasets
CALL DLT_SNOWPARK_SFRT.SIMPLE_DEMO.P_GENERATOR_PATTERN();
```

### Simple Demo Results

| Procedure | dlt Pattern | Tables | Rows |
|-----------|-------------|--------|------|
| `P_DLT_VERSION` | Version check | - | - |
| `P_CHECK_CONTEXT` | Context validation | - | - |
| `P_BASIC_DATA_LOAD` | Basic loading | TEST_ITEMS | 3 |
| `P_MULTIPLE_TABLES` | Multi-table | CUSTOMERS, ORDERS | 7 |
| `P_NESTED_JSON` | Complex structures | ECOMMERCE_ORDERS | 2 |
| `P_APPEND_MODE` | Append disposition | EVENT_LOG | 3+ |
| `P_GENERATOR_PATTERN` | Generator batches | GENERATED_NUMBERS | 500 |

**Total: 515+ rows across 6 tables**

---

## Complex Demo Examples

The Complex Demo (`examples/complex_stored_procedures.sql`) provides validation pipelines with large datasets (2000+ rows each) and complex real-world ETL patterns.

### Deploy Procedures

```bash
snow sql -c dlt-demo -q "CREATE SCHEMA IF NOT EXISTS DLT_SNOWPARK_SFRT.COMPLEX_DEMO;"
snow sql -c dlt-demo -f examples/complex_stored_procedures.sql
```

### Run and Validate

```sql
-- E-commerce data warehouse (run first for analytics)
CALL DLT_SNOWPARK_SFRT.COMPLEX_DEMO.P_ECOMMERCE_DATA_WAREHOUSE();

-- Financial trading data
CALL DLT_SNOWPARK_SFRT.COMPLEX_DEMO.P_FINANCIAL_TRADING_DATA();

-- IoT sensor telemetry
CALL DLT_SNOWPARK_SFRT.COMPLEX_DEMO.P_IOT_SENSOR_DATA();

-- Multi-source ETL with Customer 360
CALL DLT_SNOWPARK_SFRT.COMPLEX_DEMO.P_MULTI_SOURCE_ETL();

-- Data quality validation pipeline
CALL DLT_SNOWPARK_SFRT.COMPLEX_DEMO.P_DATA_QUALITY_PIPELINE();

-- E-commerce analytics (requires P_ECOMMERCE_DATA_WAREHOUSE first)
CALL DLT_SNOWPARK_SFRT.COMPLEX_DEMO.P_ECOMMERCE_ANALYTICS();
```

### Complex Demo Results

| Procedure | Pattern | Tables | Rows |
|-----------|---------|--------|------|
| `P_ECOMMERCE_DATA_WAREHOUSE` | Complex data generation | 5 | ~20,000 |
| `P_FINANCIAL_TRADING_DATA` | Time-series, financial calcs | 4 | ~5,700 |
| `P_IOT_SENSOR_DATA` | High-volume telemetry | 4 | ~4,900 |
| `P_MULTI_SOURCE_ETL` | Multi-source integration | 4 | ~12,000 |
| `P_DATA_QUALITY_PIPELINE` | Validation, quarantine | 4 | ~6,000 |
| `P_ECOMMERCE_ANALYTICS` | Read-transform-write ETL | 3 | ~2,000 |

**Total: ~50,000+ rows across 24 tables**

### Key Patterns Demonstrated

| Pattern | Description | Procedure |
|---------|-------------|-----------|
| **Large Dataset Generation** | 2000+ products with nested attributes | `P_ECOMMERCE_DATA_WAREHOUSE` |
| **Time-Series Data** | OHLCV market data, portfolio snapshots | `P_FINANCIAL_TRADING_DATA` |
| **IoT Telemetry** | Sensor readings, alerts, aggregations | `P_IOT_SENSOR_DATA` |
| **Multi-Source Integration** | CRM + ERP + Web → Customer 360 | `P_MULTI_SOURCE_ETL` |
| **Data Quality Validation** | Validation rules, clean/quarantine routing | `P_DATA_QUALITY_PIPELINE` |
| **Read-Transform-Write** | Full Snowflake-to-Snowflake ETL | `P_ECOMMERCE_ANALYTICS` |

---

## GitHub Tutorial Example

The GitHub tutorial (`examples/github_tutorial_stored_procedures.sql`) implements the [dlt Tutorial - Load Data from an API](https://dlthub.com/docs/tutorial/load-data-from-an-api) inside Snowflake.

### Setup External API Access

```sql
-- Create schema for tutorial
CREATE SCHEMA IF NOT EXISTS DLT_SNOWPARK_SFRT.GITHUB_TUTORIAL;

-- Network rule for GitHub API
CREATE OR REPLACE NETWORK RULE DLT_SNOWPARK_SFRT.GITHUB_TUTORIAL.GITHUB_API_NETWORK_RULE
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('api.github.com:443');

-- External access integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION GITHUB_API_ACCESS_INTEGRATION
  ALLOWED_NETWORK_RULES = (DLT_SNOWPARK_SFRT.GITHUB_TUTORIAL.GITHUB_API_NETWORK_RULE)
  ENABLED = TRUE
  COMMENT = 'Allows stored procedures to access GitHub API';
```

### Deploy Procedures

```bash
snow sql -c dlt-demo -f examples/github_tutorial_stored_procedures.sql
```

### Run and Validate

```sql
-- Basic API fetch
CALL DLT_SNOWPARK_SFRT.GITHUB_TUTORIAL.P_GITHUB_ISSUES_BASIC();

-- Replace mode (idempotent)
CALL DLT_SNOWPARK_SFRT.GITHUB_TUTORIAL.P_GITHUB_ISSUES_REPLACE();

-- Pagination with dlt's paginate() helper
CALL DLT_SNOWPARK_SFRT.GITHUB_TUTORIAL.P_GITHUB_ISSUES_PAGINATED();

-- Multi-table loading
CALL DLT_SNOWPARK_SFRT.GITHUB_TUTORIAL.P_GITHUB_SOURCE_MULTI_TABLE();

-- Dynamic resources
CALL DLT_SNOWPARK_SFRT.GITHUB_TUTORIAL.P_GITHUB_DYNAMIC_RESOURCES();

-- Configurable source with parameter
CALL DLT_SNOWPARK_SFRT.GITHUB_TUTORIAL.P_GITHUB_CONFIGURABLE('snowflakedb/snowpark-python');
```

### GitHub Tutorial Results

| Procedure | dlt Pattern | Tables | Rows |
|-----------|-------------|--------|------|
| `P_GITHUB_ISSUES_BASIC` | Basic API fetch | ISSUES | 30 |
| `P_GITHUB_ISSUES_REPLACE` | Replace disposition | CLOSED_ISSUES | 20 |
| `P_GITHUB_ISSUES_PAGINATED` | `paginate()` helper | ISSUES_PAGINATED | 150 |
| `P_GITHUB_SOURCE_MULTI_TABLE` | Multi-resource source | GH_ISSUES, GH_COMMENTS | 50 |
| `P_GITHUB_DYNAMIC_RESOURCES` | Dynamic resources | DYN_ISSUES, DYN_PULLS, DYN_RELEASES | 50 |
| `P_GITHUB_CONFIGURABLE` | Parameterized source | CFG_REPO_INFO, CFG_ISSUES | 11 |

**Total: 311 rows across 10 tables**

---

## Transformers Tutorial Example

The Transformers tutorial (`examples/transformers_tutorial_stored_procedures.sql`) implements the [dlt Transformers Example](https://github.com/dlt-hub/dlt/tree/master/docs/examples/transformers) inside Snowflake, demonstrating advanced dlt patterns using the PokeAPI.

### Setup External API Access

```sql
-- Create schema for tutorial
CREATE SCHEMA IF NOT EXISTS DLT_SNOWPARK_SFRT.TRANSFORMERS_TUTORIAL;

-- Network rule for PokeAPI
CREATE OR REPLACE NETWORK RULE DLT_SNOWPARK_SFRT.TRANSFORMERS_TUTORIAL.POKEAPI_NETWORK_RULE
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('pokeapi.co:443');

-- External access integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION POKEAPI_ACCESS_INTEGRATION
  ALLOWED_NETWORK_RULES = (DLT_SNOWPARK_SFRT.TRANSFORMERS_TUTORIAL.POKEAPI_NETWORK_RULE)
  ENABLED = TRUE
  COMMENT = 'Allows stored procedures to access PokeAPI';
```

### Deploy Procedures

```bash
snow sql -c dlt-demo -f examples/transformers_tutorial_stored_procedures.sql
```

### Run and Validate

```sql
-- Basic Pokemon list
CALL DLT_SNOWPARK_SFRT.TRANSFORMERS_TUTORIAL.P_POKEMON_LIST_BASIC();

-- Pokemon with details (transformer pattern)
CALL DLT_SNOWPARK_SFRT.TRANSFORMERS_TUTORIAL.P_POKEMON_WITH_DETAILS();

-- Full transformer chain
CALL DLT_SNOWPARK_SFRT.TRANSFORMERS_TUTORIAL.P_POKEMON_FULL_TRANSFORMER_CHAIN();

-- Parallel fetch pattern
CALL DLT_SNOWPARK_SFRT.TRANSFORMERS_TUTORIAL.P_POKEMON_PARALLEL_FETCH();

-- Dynamic transformers
CALL DLT_SNOWPARK_SFRT.TRANSFORMERS_TUTORIAL.P_POKEMON_DYNAMIC_TRANSFORMERS();

-- add_map pattern
CALL DLT_SNOWPARK_SFRT.TRANSFORMERS_TUTORIAL.P_POKEMON_ADD_MAP_PATTERN();
```

### Transformers Tutorial Results

| Procedure | dlt Pattern | Tables | Rows |
|-----------|-------------|--------|------|
| `P_POKEMON_LIST_BASIC` | Basic API fetch | POKEMON_LIST | 20 |
| `P_POKEMON_WITH_DETAILS` | Transformer pattern | POKEMON_DETAILS | 5 |
| `P_POKEMON_FULL_TRANSFORMER_CHAIN` | Chained transformers | POKEMON_WITH_STATS | 5 |
| `P_POKEMON_PARALLEL_FETCH` | ThreadPoolExecutor | POKEMON_PARALLEL | 10 |
| `P_POKEMON_DYNAMIC_TRANSFORMERS` | Dynamic resources | POKEMON_TYPE_* | 15 |
| `P_POKEMON_ADD_MAP_PATTERN` | `add_map()` function | POKEMON_MAPPED | 5 |

**Total: 60+ rows across 6+ tables**

### Key dlt Patterns Demonstrated

| Pattern | Description | Procedure |
|---------|-------------|-----------|
| **Transformers** | Enrich data by fetching additional details | `P_POKEMON_WITH_DETAILS` |
| **Chained Transformers** | Multiple transformation steps in sequence | `P_POKEMON_FULL_TRANSFORMER_CHAIN` |
| **Parallel Fetch** | Use ThreadPoolExecutor for concurrent API calls | `P_POKEMON_PARALLEL_FETCH` |
| **Dynamic Resources** | Create resources programmatically based on data | `P_POKEMON_DYNAMIC_TRANSFORMERS` |
| **add_map()** | Apply transformations to resource items | `P_POKEMON_ADD_MAP_PATTERN` |

---

## Chess Production Example

The Chess Production tutorial (`examples/chess_production_stored_procedures.sql`) implements the [dlt Chess Production Example](https://dlthub.com/docs/examples/chess_production) inside Snowflake, demonstrating production-grade dlt patterns using the Chess.com API.

### Setup External API Access

```sql
-- Create schema for tutorial
CREATE SCHEMA IF NOT EXISTS DLT_SNOWPARK_SFRT.CHESS_PRODUCTION;

-- Network rule for Chess.com API
CREATE OR REPLACE NETWORK RULE DLT_SNOWPARK_SFRT.CHESS_PRODUCTION.CHESS_API_NETWORK_RULE
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('api.chess.com:443');

-- External access integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION CHESS_API_ACCESS_INTEGRATION
  ALLOWED_NETWORK_RULES = (DLT_SNOWPARK_SFRT.CHESS_PRODUCTION.CHESS_API_NETWORK_RULE)
  ENABLED = TRUE
  COMMENT = 'Allows stored procedures to access Chess.com API';
```

### Deploy Procedures

```bash
snow sql -c dlt-demo -f examples/chess_production_stored_procedures.sql
```

### Run and Validate

```sql
-- Basic players list
CALL DLT_SNOWPARK_SFRT.CHESS_PRODUCTION.P_CHESS_PLAYERS_BASIC('GM', 5);

-- Player profiles with parallel fetching
CALL DLT_SNOWPARK_SFRT.CHESS_PRODUCTION.P_CHESS_PLAYERS_PROFILES('GM', 5);

-- Player games for a specific month
CALL DLT_SNOWPARK_SFRT.CHESS_PRODUCTION.P_CHESS_PLAYERS_GAMES('GM', 3, 2024, 12);

-- Full production pipeline (players + profiles + games)
CALL DLT_SNOWPARK_SFRT.CHESS_PRODUCTION.P_CHESS_FULL_PIPELINE('GM', 5, 2024, 12);

-- Multi-title players (GM, IM, FM, WGM)
CALL DLT_SNOWPARK_SFRT.CHESS_PRODUCTION.P_CHESS_MULTI_TITLE(3);

-- Player statistics with ratings
CALL DLT_SNOWPARK_SFRT.CHESS_PRODUCTION.P_CHESS_STATS('GM', 5);
```

### Chess Production Results

| Procedure | dlt Pattern | Tables | Rows |
|-----------|-------------|--------|------|
| `P_CHESS_PLAYERS_BASIC` | Basic API fetch | PLAYERS | 5 |
| `P_CHESS_PLAYERS_PROFILES` | Parallel transformer | PLAYERS_PROFILES | 5 |
| `P_CHESS_PLAYERS_GAMES` | Transformer pattern | PLAYERS_GAMES | 100+ |
| `P_CHESS_FULL_PIPELINE` | Multi-resource pipeline | PLAYERS, PLAYERS_PROFILES, PLAYERS_GAMES | 200+ |
| `P_CHESS_MULTI_TITLE` | Dynamic resources | TITLED_PLAYERS, TITLED_PROFILES | 20+ |
| `P_CHESS_STATS` | Parallel transformer | PLAYER_STATS | 5 |

**Total: 300+ rows across 6+ tables**

### Key dlt Production Patterns Demonstrated

| Pattern | Description | Procedure |
|---------|-------------|-----------|
| **Base Resource** | Fetch list of items to process | `P_CHESS_PLAYERS_BASIC` |
| **Parallel Transformers** | Use ThreadPoolExecutor for concurrent API calls | `P_CHESS_PLAYERS_PROFILES` |
| **Chained Transformers** | Process games for each player | `P_CHESS_PLAYERS_GAMES` |
| **Multi-Resource Pipeline** | Load multiple tables in single pipeline run | `P_CHESS_FULL_PIPELINE` |
| **Dynamic Resources** | Create resources for multiple categories | `P_CHESS_MULTI_TITLE` |
| **Nested Stats Extraction** | Flatten complex API responses | `P_CHESS_STATS` |

---

## Why This Approach?

### The Problem

dlt's built-in Snowflake destination fails inside stored procedures:

```python
# This FAILS inside stored procedures!
pipeline = dlt.pipeline(
    pipeline_name="test_pipeline",
    destination=dlt.destinations.snowflake(credentials={...}),
)
# Error: 090236 (42601): Unsupported statement type 'PUT_FILES'
```

### Root Cause

1. dlt's Snowflake destination uses `PUT` command to upload files to internal stage
2. Then uses `COPY INTO` to load data from stage to tables
3. **The `PUT` command is NOT supported inside Snowflake stored procedures**
4. This is a fundamental Snowflake limitation, not a dlt issue

### The Solution

Use dlt's custom destination API with Snowpark's `save_as_table()`:

```python
import dlt
from dlt_snowpark import DltSnowparkLoader

loader = DltSnowparkLoader(session, pipeline_name="test_pipeline")

@dlt.resource(name="my_table", write_disposition="replace")
def my_data():
    yield [{"id": 1}]

return loader.run_json(my_data())
```

**Benefits:**
- ✅ No external credentials (uses session context)
- ✅ No EXTERNAL_ACCESS_INTEGRATIONS for data loading
- ✅ No network access needed for writes
- ✅ Works perfectly

---

## API Reference

### DltSnowparkLoader

```python
from dlt_snowpark import DltSnowparkLoader

loader = DltSnowparkLoader(
    session,                           # Snowpark session from SP
    pipeline_name="my_pipeline",       # Optional (default: "snowpark_pipeline")
    batch_size=5000                    # Optional (default: 5000)
)
```

#### Methods

| Method | Description |
|--------|-------------|
| `loader.run(*resources)` | Run pipeline, returns dict with status, tables, errors |
| `loader.run_json(*resources)` | Run pipeline, returns formatted JSON string |
| `loader.verify_table(name, limit=5)` | Query table to verify data |

> **Note:** Use `@dlt.resource()` decorator directly from dlt (requires `import dlt`).

#### Properties

| Property | Description |
|----------|-------------|
| `loader.database` | Current database name |
| `loader.schema` | Current schema name |
| `loader.loaded_tables` | Dict of table names to row counts |
| `loader.errors` | List of error messages |

#### Return Value

```python
{
    "status": "success" | "partial" | "error",
    "database": "DLT_SNOWPARK_SFRT",
    "schema": "GITHUB_TUTORIAL",
    "tables_loaded": {"ISSUES": 30, "COMMENTS": 20},
    "total_rows": 50,
    "errors": None,
    "load_info": {
        "pipeline_name": "my_pipeline",
        "started_at": "2026-01-03T10:00:00",
        "finished_at": "2026-01-03T10:00:01"
    }
}
```

---

## Examples

### Simple Data Load

```python
def my_procedure(session):
    import dlt
    from dlt_snowpark import DltSnowparkLoader
    
    loader = DltSnowparkLoader(session)
    
    @dlt.resource(name="users", write_disposition="replace")
    def get_users():
        yield [{"id": 1, "name": "Alice"}]
    
    return loader.run_json(get_users())
```

### Multiple Tables

```python
def load_all(session):
    import dlt
    from dlt_snowpark import DltSnowparkLoader
    
    loader = DltSnowparkLoader(session, pipeline_name="multi_table")
    
    @dlt.resource(name="customers", write_disposition="replace")
    def customers():
        yield [{"id": 1, "name": "Acme Corp"}]
    
    @dlt.resource(name="orders", write_disposition="replace")
    def orders():
        yield [{"order_id": 100, "customer_id": 1}]
    
    return loader.run_json(customers(), orders())
```

### External API with Pagination

```python
def fetch_github_issues(session):
    import dlt
    from dlt_snowpark import DltSnowparkLoader
    from dlt.sources.helpers.rest_client import paginate
    
    loader = DltSnowparkLoader(session)
    
    @dlt.resource(name="issues", write_disposition="replace")
    def get_issues():
        for page in paginate(
            "https://api.github.com/repos/dlt-hub/dlt/issues",
            params={"per_page": 50, "state": "all"}
        ):
            yield [{"id": i["id"], "title": i["title"]} for i in page]
    
    return loader.run_json(get_issues())
```

### Full ETL: Read → Transform → Write

```python
def build_analytics(session):
    import dlt
    import json
    from dlt_snowpark import DltSnowparkLoader
    
    loader = DltSnowparkLoader(session, pipeline_name="analytics")
    db, schema = loader.database, loader.schema
    
    # Read and transform with SQL
    analytics_df = session.sql(f"""
        SELECT customer_id, COUNT(*) as order_count
        FROM {db}.{schema}.orders
        GROUP BY customer_id
    """)
    data = [row.as_dict() for row in analytics_df.collect()]
    
    # Write using dlt
    @dlt.resource(name="customer_analytics", write_disposition="replace")
    def customer_analytics():
        yield data
    
    return loader.run_json(customer_analytics())
```

---

## Feature Compatibility

| dlt Feature | Standard Snowflake Dest | DltSnowparkLoader |
|-------------|------------------------|-------------------|
| Basic data loading | ❌ (PUT blocked) | ✅ Works |
| Resources & Sources | ❌ | ✅ Works |
| Schema inference | ❌ | ✅ Works (all as strings) |
| Replace disposition | ❌ | ✅ Works |
| Append disposition | ❌ | ✅ Works |
| `paginate()` helper | ❌ | ✅ Works |
| Dynamic resources | ❌ | ✅ Works |
| Parallel fetch | ❌ | ✅ Works (ThreadPoolExecutor) |
| External API calls | ❌ | ✅ Works (with EAI) |
| State management | ❌ | ⚠️ Custom impl needed |
| Incremental loading | ❌ | ⚠️ Custom impl needed |
| Type preservation | ❌ | ⚠️ All stored as strings |

---

## Deployment with Snowflake CLI

### Initial Setup

```bash
# Test connection
snow connection test -c dlt-demo

# Create database structure
snow sql -c dlt-demo -q "
CREATE DATABASE IF NOT EXISTS DLT_SNOWPARK_SFRT;
CREATE SCHEMA IF NOT EXISTS DLT_SNOWPARK_SFRT.DLT_SNOWPARK;
CREATE STAGE IF NOT EXISTS DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES;
"

# Upload helper module
snow stage copy dlt_snowpark.py @DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/ -c dlt-demo --overwrite

# Deploy Simple Demo procedures (no external API needed)
snow sql -c dlt-demo -f examples/simple_demo_stored_procedures.sql

# Deploy Complex Demo procedures (large datasets)
snow sql -c dlt-demo -f examples/complex_stored_procedures.sql

# Deploy GitHub tutorial procedures
snow sql -c dlt-demo -f examples/github_tutorial_stored_procedures.sql

# Deploy Transformers tutorial procedures
snow sql -c dlt-demo -f examples/transformers_tutorial_stored_procedures.sql

# Deploy Chess Production tutorial procedures
snow sql -c dlt-demo -f examples/chess_production_stored_procedures.sql

# Run a procedure
snow sql -c dlt-demo -q "CALL DLT_SNOWPARK_SFRT.SIMPLE_DEMO.P_BASIC_DATA_LOAD();"
```

### Updating the Module

When updating `dlt_snowpark.py`:

```bash
# Re-upload the module
snow stage copy dlt_snowpark.py @DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/ -c dlt-demo --overwrite

# Re-create stored procedures (they cache imports)
snow sql -c dlt-demo -f examples/simple_demo_stored_procedures.sql
snow sql -c dlt-demo -f examples/complex_stored_procedures.sql
snow sql -c dlt-demo -f examples/github_tutorial_stored_procedures.sql
snow sql -c dlt-demo -f examples/transformers_tutorial_stored_procedures.sql
snow sql -c dlt-demo -f examples/chess_production_stored_procedures.sql

# Test
snow sql -c dlt-demo -q "CALL DLT_SNOWPARK_SFRT.SIMPLE_DEMO.P_BASIC_DATA_LOAD();"
snow sql -c dlt-demo -q "CALL DLT_SNOWPARK_SFRT.COMPLEX_DEMO.P_DATA_QUALITY_PIPELINE();"
snow sql -c dlt-demo -q "CALL DLT_SNOWPARK_SFRT.GITHUB_TUTORIAL.P_GITHUB_ISSUES_BASIC();"
snow sql -c dlt-demo -q "CALL DLT_SNOWPARK_SFRT.TRANSFORMERS_TUTORIAL.P_POKEMON_LIST_BASIC();"
snow sql -c dlt-demo -q "CALL DLT_SNOWPARK_SFRT.CHESS_PRODUCTION.P_CHESS_PLAYERS_BASIC();"
```

---

## Project Structure

```
dlt-snowflake/
├── dlt_snowpark.py                    # Main helper module - upload to stage
├── sfrt_stored_procedures.sql         # SFRT schema procedure
├── docs/
│   └── dlt-in-snowpark.md             # This documentation
└── examples/
    ├── simple_demo_stored_procedures.sql      # 7 simple demo procedures
    ├── complex_stored_procedures.sql          # 6 complex validation pipelines
    ├── github_tutorial_stored_procedures.sql  # 6 GitHub API tutorial procedures
    ├── transformers_tutorial_stored_procedures.sql  # 6 Pokemon transformers
    └── chess_production_stored_procedures.sql # 6 Chess.com production pipeline
```

| File | Description |
|------|-------------|
| `dlt_snowpark.py` | Reusable helper module - upload to stage |
| `sfrt_stored_procedures.sql` | Basic SFRT schema example procedure |
| `examples/simple_demo_stored_procedures.sql` | 7 simple demo procedures (no external API) |
| `examples/complex_stored_procedures.sql` | 6 complex validation pipelines (~50K rows) |
| `examples/github_tutorial_stored_procedures.sql` | 6 GitHub API tutorial procedures |
| `examples/transformers_tutorial_stored_procedures.sql` | 6 Pokemon transformers tutorial procedures |
| `examples/chess_production_stored_procedures.sql` | 6 Chess.com production pipeline procedures |
| `docs/dlt-in-snowpark.md` | This documentation |

---

## Issues Fixed During Development

### NULL Values Converted to String "None"

```python
# WRONG
{k.upper(): str(v) for k, v in item.items()}  # None → "None"

# CORRECT
{k.upper(): (str(v) if v is not None else None) for k, v in item.items()}
```

### Column Count Mismatch Between Batches

Different items may have different keys. Normalize all items:

```python
all_columns = set()
for item in items:
    all_columns.update(item.keys())

normalized = [{col: item.get(col) for col in all_columns} for item in items]
```

### Schema Evolution on Append

Subsequent batches must align with existing table schema:

```python
if is_first_batch:
    snowpark_df.write.mode("overwrite").save_as_table(table_name)
else:
    existing_cols = [f.name for f in session.table(table_name).schema.fields]
    aligned_df = snowpark_df.select(*[df[c] for c in existing_cols])
    aligned_df.write.mode("append").save_as_table(table_name)
```

---

## Limitations

1. **All data stored as strings** - Type preservation requires additional handling
2. **No incremental state** - dlt's built-in state management is bypassed
3. **Memory constraints** - Very large datasets should be processed in chunks
4. **First batch determines schema** - Table schema derived from first batch

---

## Key Takeaways

1. **dlt's Snowflake destination relies on `PUT` command** which is blocked in stored procedures
2. **Use DltSnowparkLoader** for a clean, reusable solution
3. **Snowpark's `save_as_table()`** is the way to write data from within stored procedures
4. **No pandas needed** — Snowpark's `create_dataframe()` accepts list of dicts directly
5. **All dlt ETL features** (sources, resources, transformations) still work
6. **External APIs work** — Use EXTERNAL_ACCESS_INTEGRATIONS for GitHub, PokeAPI, Chess.com, etc.
7. **dlt's `paginate()` works** — REST client helpers function correctly inside SPs
8. **Minimal code overhead** — Only ~2 extra lines compared to native dlt

---

## References

### dlt Documentation
- [dlt Documentation](https://dlthub.com/docs/intro)
- [dlt Tutorial - Load Data from an API](https://dlthub.com/docs/tutorial/load-data-from-an-api)
- [dlt Transformers Example](https://github.com/dlt-hub/dlt/tree/master/docs/examples/transformers)
- [dlt Chess Production Example](https://dlthub.com/docs/examples/chess_production)
- [dlt Custom Destinations](https://dlthub.com/docs/dlt-ecosystem/destinations/destination)
- [dlt REST Client Helpers](https://dlthub.com/docs/general-usage/http/rest-client)

### Snowflake Documentation
- [Snowflake Python Stored Procedures](https://docs.snowflake.com/en/developer-guide/stored-procedure/python/procedure-python-overview)
- [Snowflake PUT Command Limitations](https://docs.snowflake.com/en/sql-reference/sql/put)
- [Snowpark create_dataframe](https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/latest/snowpark/api/snowflake.snowpark.Session.createDataFrame)
- [Snowflake External Network Access](https://docs.snowflake.com/en/developer-guide/external-network-access/external-network-access-overview)

---

*Document created: January 3, 2026*  
*Last updated: January 3, 2026*  
*Database: DLT_SNOWPARK_SFRT*  
*Validated with Snowflake CLI (dlt-demo connection)*
