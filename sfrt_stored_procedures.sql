-- SFRT Schema Setup and Stored Procedures
-- Uses dlt_snowpark.py helper module for running dlt inside Snowflake stored procedures

-- Create schema
CREATE SCHEMA IF NOT EXISTS DLT_SNOWPARK_SFRT.SFRT;

-- Test dlt with Snowpark destination in stored procedure
CREATE OR REPLACE PROCEDURE DLT_SNOWPARK_SFRT.SFRT.P_TEST_DLT_SNOWFLAKE()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'test_dlt'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python', 'dlt')
IMPORTS = ('@DLT_SNOWPARK_SFRT.DLT_SNOWPARK.MODULES/dlt_snowpark.py')
COMMENT = 'Test dlt with Snowflake destination in stored procedure'
AS
$$
def test_dlt(snowpark_session):
    from dlt_snowpark import DltSnowparkLoader
    import json

    try:
        # Simple test data - just a few JSON documents
        test_data = [
            {"id": 1, "name": "Test Item 1", "created_at": "2026-01-01T10:00:00Z"},
            {"id": 2, "name": "Test Item 2", "created_at": "2026-01-02T11:00:00Z"},
            {"id": 3, "name": "Test Item 3", "created_at": "2026-01-03T12:00:00Z"}
        ]

        # Configure DltSnowparkLoader
        loader = DltSnowparkLoader(snowpark_session, pipeline_name="test_pipeline")

        # Create a simple dlt resource
        @loader.resource(name="test_table", write_disposition="replace")
        def test_resource():
            yield test_data

        # Run the pipeline
        result = loader.run(test_resource())

        # Format result to match original structure
        output = {
            "status": result["status"],
            "rows_loaded": result["total_rows"],
            "load_info": result["load_info"]
        }

        return json.dumps(output, indent=2)

    except Exception as e:
        import traceback
        error_result = {
            "status": "error",
            "error_type": type(e).__name__,
            "error_message": str(e),
            "traceback": traceback.format_exc()
        }
        return json.dumps(error_result, indent=2)
$$;

