-- Simple Demo Stored Procedures
-- Uses DltSnowparkLoader helper for clean, maintainable dlt pipelines
-- Deployed to DLT_SNOWPARK_SFRT.SIMPLE_DEMO schema

--------------------------------------------------------------------------------
-- 1. P_BASIC_DATA_LOAD
-- Demonstrates basic data loading with dlt
--------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.SIMPLE_DEMO."P_BASIC_DATA_LOAD"()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python','dlt')
HANDLER = 'main'
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
COMMENT = 'Basic data loading example using DltSnowparkLoader'
EXECUTE AS OWNER
AS '
def main(session):
    """
    Basic Data Load Example
    
    Loads a simple list of items into Snowflake using dlt patterns.
    """
    import json
    from dlt_snowpark import DltSnowparkLoader
    
    loader = DltSnowparkLoader(session, pipeline_name="basic_demo")
    
    # Sample data
    test_data = [
        {"id": 1, "name": "Test Item 1", "created_at": "2026-01-01T10:00:00Z"},
        {"id": 2, "name": "Test Item 2", "created_at": "2026-01-02T11:00:00Z"},
        {"id": 3, "name": "Test Item 3", "created_at": "2026-01-03T12:00:00Z"}
    ]
    
    @loader.resource(name="test_items", write_disposition="replace")
    def get_test_items():
        yield test_data
    
    result = loader.run(get_test_items())
    verification = loader.verify_table("test_items")
    
    return json.dumps({
        "load_result": result,
        "verification": verification
    }, indent=2, default=str)
';

--------------------------------------------------------------------------------
-- 2. P_MULTIPLE_TABLES
-- Demonstrates loading multiple tables in a single pipeline run
--------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.SIMPLE_DEMO."P_MULTIPLE_TABLES"()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python','dlt')
HANDLER = 'main'
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
COMMENT = 'Demonstrates loading multiple tables in one pipeline'
EXECUTE AS OWNER
AS '
def main(session):
    """
    Multiple Tables Example
    
    Shows how to load multiple related tables (customers and orders) 
    in a single pipeline run.
    """
    import json
    from dlt_snowpark import DltSnowparkLoader
    
    loader = DltSnowparkLoader(session, pipeline_name="multi_table_demo")
    
    @loader.resource(name="customers", write_disposition="replace")
    def get_customers():
        yield [
            {"id": 1, "name": "Alice Johnson", "email": "alice@example.com"},
            {"id": 2, "name": "Bob Smith", "email": "bob@example.com"},
            {"id": 3, "name": "Charlie Brown", "email": "charlie@example.com"}
        ]
    
    @loader.resource(name="orders", write_disposition="replace")
    def get_orders():
        yield [
            {"order_id": 1001, "customer_id": 1, "amount": 99.99, "status": "shipped"},
            {"order_id": 1002, "customer_id": 1, "amount": 149.50, "status": "pending"},
            {"order_id": 1003, "customer_id": 2, "amount": 75.00, "status": "delivered"},
            {"order_id": 1004, "customer_id": 3, "amount": 200.00, "status": "shipped"}
        ]
    
    result = loader.run(get_customers(), get_orders())
    
    customers_verification = loader.verify_table("customers")
    orders_verification = loader.verify_table("orders")
    
    return json.dumps({
        "load_result": result,
        "customers": customers_verification,
        "orders": orders_verification
    }, indent=2, default=str)
';

--------------------------------------------------------------------------------
-- 3. P_NESTED_JSON
-- Demonstrates handling nested JSON data
--------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.SIMPLE_DEMO."P_NESTED_JSON"()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python','dlt')
HANDLER = 'main'
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
COMMENT = 'Demonstrates handling nested JSON data'
EXECUTE AS OWNER
AS $$
def main(session):
    """
    Nested JSON Example

    Shows how to handle complex nested JSON data with dlt.
    The loader flattens nested structures for storage.
    """
    import json
    from datetime import datetime
    from dlt_snowpark import DltSnowparkLoader

    loader = DltSnowparkLoader(session, pipeline_name="nested_json_demo")

    @loader.resource(name="ecommerce_orders", write_disposition="replace")
    def get_orders():
        yield [
            {
                "id": 1001,
                "customer_name": "Alice Johnson",
                "customer_email": "alice@example.com",
                "shipping_street": "123 Main St",
                "shipping_city": "Seattle",
                "shipping_country": "USA",
                "item_sku": "LAPTOP-001",
                "item_name": "MacBook Pro",
                "item_price": 2499.99,
                "payment_method": "credit_card",
                "payment_status": "completed",
                "created_at": datetime.now().isoformat(),
                "tags": "premium,tech,expedited"
            },
            {
                "id": 1002,
                "customer_name": "Bob Smith",
                "customer_email": "bob@example.com",
                "shipping_street": "456 Oak Ave",
                "shipping_city": "Portland",
                "shipping_country": "USA",
                "item_sku": "PHONE-007",
                "item_name": "iPhone 15",
                "item_price": 999.99,
                "payment_method": "paypal",
                "payment_status": "pending",
                "created_at": datetime.now().isoformat(),
                "tags": "mobile"
            }
        ]

    result = loader.run(get_orders())
    verification = loader.verify_table("ecommerce_orders")

    return json.dumps({
        "load_result": result,
        "verification": verification
    }, indent=2, default=str)
$$;

--------------------------------------------------------------------------------
-- 4. P_APPEND_MODE
-- Demonstrates append write disposition
--------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.SIMPLE_DEMO."P_APPEND_MODE"()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python','dlt')
HANDLER = 'main'
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
COMMENT = 'Demonstrates append write disposition for incremental loading'
EXECUTE AS OWNER
AS '
def main(session):
    """
    Append Mode Example
    
    Shows how to use append write disposition to add data without
    replacing existing records. Each call adds more log entries.
    """
    import json
    from datetime import datetime
    from dlt_snowpark import DltSnowparkLoader
    
    loader = DltSnowparkLoader(session, pipeline_name="append_demo")
    
    # Generate a unique batch of log entries
    timestamp = datetime.now().isoformat()
    
    @loader.resource(name="event_log", write_disposition="append")
    def get_events():
        yield [
            {"event_id": f"evt_{timestamp}_1", "event_type": "page_view", "user_id": 101, "timestamp": timestamp},
            {"event_id": f"evt_{timestamp}_2", "event_type": "click", "user_id": 102, "timestamp": timestamp},
            {"event_id": f"evt_{timestamp}_3", "event_type": "purchase", "user_id": 101, "timestamp": timestamp}
        ]
    
    result = loader.run(get_events())
    verification = loader.verify_table("event_log")
    
    return json.dumps({
        "load_result": result,
        "verification": verification,
        "note": "Call this procedure multiple times to see rows accumulate"
    }, indent=2, default=str)
';

--------------------------------------------------------------------------------
-- 5. P_GENERATOR_PATTERN
-- Demonstrates using generators for large datasets
--------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.SIMPLE_DEMO."P_GENERATOR_PATTERN"()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python','dlt')
HANDLER = 'main'
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
COMMENT = 'Demonstrates using generators for memory-efficient loading'
EXECUTE AS OWNER
AS '
def main(session):
    """
    Generator Pattern Example
    
    Shows how to use Python generators for memory-efficient loading
    of larger datasets. dlt processes data in batches.
    """
    import json
    from dlt_snowpark import DltSnowparkLoader
    
    loader = DltSnowparkLoader(session, pipeline_name="generator_demo")
    
    @loader.resource(name="generated_numbers", write_disposition="replace")
    def generate_numbers():
        # Yield data in batches for memory efficiency
        batch_size = 100
        total_items = 500
        
        for start in range(0, total_items, batch_size):
            batch = []
            for i in range(start, min(start + batch_size, total_items)):
                batch.append({
                    "id": i + 1,
                    "value": i * 2,
                    "squared": i ** 2,
                    "is_even": i % 2 == 0
                })
            yield batch
    
    result = loader.run(generate_numbers())
    verification = loader.verify_table("generated_numbers")
    
    return json.dumps({
        "load_result": result,
        "verification": verification
    }, indent=2, default=str)
';

--------------------------------------------------------------------------------
-- 6. P_CHECK_CONTEXT
-- Debug procedure to check session context
--------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.SIMPLE_DEMO."P_CHECK_CONTEXT"()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python','dlt')
HANDLER = 'main'
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
COMMENT = 'Debug procedure to check session context and dlt version'
EXECUTE AS OWNER
AS '
def main(session):
    """
    Context Check Example
    
    Shows session context information and validates that
    the DltSnowparkLoader is properly imported.
    """
    import json
    import dlt
    from dlt_snowpark import DltSnowparkLoader
    
    # Create loader to access helper methods
    loader = DltSnowparkLoader(session, pipeline_name="context_check")
    
    context = {
        "dlt_version": dlt.__version__,
        "account": str(session.get_current_account()).strip(''"''),
        "database": loader.database,
        "schema": loader.schema,
        "warehouse": str(session.get_current_warehouse()).strip(''"''),
        "role": str(session.get_current_role()).strip(''"''),
        "user": str(session.get_current_user()).strip(''"''),
        "helper_module": "DltSnowparkLoader loaded successfully"
    }
    
    return json.dumps(context, indent=2)
';

--------------------------------------------------------------------------------
-- 7. P_DLT_VERSION
-- Simple procedure to check dlt version
--------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.SIMPLE_DEMO."P_DLT_VERSION"()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python','dlt')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
def main(session):
    """Returns the dlt version installed in Snowflake."""
    import dlt
    return f"dlt version: {dlt.__version__}"
';

