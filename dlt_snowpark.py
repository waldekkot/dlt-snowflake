"""
dlt_snowpark.py - Reusable dlt helper for Snowflake Stored Procedures

This module provides a workaround for running dlt inside Snowflake stored procedures.
dlt's built-in Snowflake destination uses the PUT command which is NOT supported
inside stored procedures. This module uses a custom destination with Snowpark's
native save_as_table() method instead.

SETUP:
    1. Upload this file to a Snowflake stage:
       PUT file:///path/to/dlt_snowpark.py @my_stage/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

    2. Reference it in your stored procedure using IMPORTS clause:
       IMPORTS = ('@my_stage/dlt_snowpark.py')

USAGE:
    from dlt_snowpark import DltSnowparkLoader

    loader = DltSnowparkLoader(session, pipeline_name="my_pipeline")

    @loader.resource(name="my_table")
    def my_data():
        yield [{"id": 1, "name": "test"}]

    return loader.run_json(my_data())

WHY THIS EXISTS:
    - dlt's Snowflake destination relies on PUT command to upload files to internal stage
    - The PUT command is NOT supported inside Snowflake stored procedures
    - Error: "090236 (42601): Stored procedure execution error: Unsupported statement type 'PUT_FILES'"
    - This module bypasses that limitation by using Snowpark's create_dataframe() and save_as_table()

FEATURES:
    - All dlt ETL features work (resources, sources, transformations, normalization)
    - No external credentials needed - uses the session's context
    - No EXTERNAL_ACCESS_INTEGRATIONS required
    - No pandas dependency
    - Supports replace and append write dispositions

LIMITATIONS:
    - State management must be handled separately if needed
    - Incremental loading requires custom implementation
    - All columns are stored as strings (schema inference from dlt is bypassed)

Author: Based on experimentation with dlt inside Snowflake Python stored procedures
Date: January 2026
"""

from __future__ import annotations

import json
import traceback
from typing import TYPE_CHECKING, Any

import dlt
from dlt.common.schema.typing import TTableSchema
from dlt.common.typing import TDataItems

# Explicit public API
__all__ = ["DltSnowparkLoader"]

# Type hints for Snowpark session (avoid runtime import)
if TYPE_CHECKING:
    from snowflake.snowpark import Session


class DltSnowparkLoader:
    """
    Elegant wrapper for running dlt pipelines inside Snowflake stored procedures.

    This class provides a clean interface for loading data using dlt's powerful
    ETL capabilities while working around Snowflake's PUT command limitation
    in stored procedures.

    Attributes:
        session: Snowpark session passed from the stored procedure
        pipeline_name: Name for the dlt pipeline (used for state management)
        loaded_tables: Dictionary tracking rows loaded per table
        errors: List of any errors encountered during loading

    Example:
        def my_stored_procedure(session):
            from dlt_snowpark import DltSnowparkLoader

            loader = DltSnowparkLoader(session, pipeline_name="customer_pipeline")

            @loader.resource(name="customers", write_disposition="replace")
            def get_customers():
                # Your data fetching logic here
                yield [
                    {"id": 1, "name": "Alice", "email": "alice@example.com"},
                    {"id": 2, "name": "Bob", "email": "bob@example.com"},
                ]

            return loader.run_json(get_customers())
    """

    __slots__ = (
        "session",
        "pipeline_name",
        "batch_size",
        "loaded_tables",
        "errors",
        "_db",
        "_schema",
    )

    def __init__(
        self,
        session: Session,
        pipeline_name: str = "snowpark_pipeline",
        batch_size: int = 5000,
    ) -> None:
        """
        Initialize the DltSnowparkLoader.

        Args:
            session: Snowpark session from the stored procedure
            pipeline_name: Name for the dlt pipeline (default: "snowpark_pipeline")
            batch_size: Number of items per batch for the custom destination (default: 5000)
        """
        self.session = session
        self.pipeline_name = pipeline_name
        self.batch_size = batch_size
        self.loaded_tables: dict[str, int] = {}
        self.errors: list[str] = []

        # Get current database and schema context
        self._db = str(session.get_current_database()).strip('"')
        self._schema = str(session.get_current_schema()).strip('"')

    @property
    def database(self) -> str:
        """Current database name."""
        return self._db

    @property
    def schema(self) -> str:
        """Current schema name."""
        return self._schema

    def _create_destination(self):
        """
        Creates a custom dlt destination using Snowpark's save_as_table().

        This is the core workaround - instead of using dlt's built-in Snowflake
        destination (which uses PUT command), we create a custom destination
        that writes data directly using Snowpark DataFrames.

        Returns:
            A dlt destination function decorated with @dlt.destination
        """
        # Capture instance variables for closure
        session = self.session
        db = self._db
        schema = self._schema
        loaded_tables = self.loaded_tables
        errors = self.errors

        @dlt.destination(batch_size=self.batch_size, naming_convention="snake_case")
        def snowpark_destination(items: TDataItems, table: TTableSchema) -> None:
            """Custom destination that writes to Snowflake using Snowpark."""
            table_name = table["name"].upper()

            # Skip dlt internal tables
            if table_name.startswith("_DLT"):
                return

            # Skip empty batches
            if not items or not isinstance(items, list):
                return

            # Track loaded rows using setdefault
            loaded_tables.setdefault(table_name, 0)
            loaded_tables[table_name] += len(items)

            full_table_name = f"{db}.{schema}.{table_name}"

            try:
                # Convert to uppercase column names and string values
                # Keep None as None (NULL) - don't convert to string "None"
                items_processed = [
                    {
                        k.upper(): (str(v) if v is not None else None)
                        for k, v in item.items()
                    }
                    for item in items
                ]

                if not items_processed:
                    return

                # Get ALL column names from ALL items in this batch
                all_columns: set[str] = set()
                for item in items_processed:
                    all_columns.update(item.keys())

                # Normalize all items to have the same keys (fill missing with None)
                normalized_items = []
                for item in items_processed:
                    normalized = {col: item.get(col) for col in all_columns}
                    normalized_items.append(normalized)

                # Create Snowpark DataFrame from normalized dicts
                snowpark_df = session.create_dataframe(normalized_items)

                # Determine mode: overwrite for first batch, append for subsequent
                is_first_batch = loaded_tables[table_name] == len(items)

                if is_first_batch:
                    # First batch: drop table if exists then create new
                    session.sql(f"DROP TABLE IF EXISTS {full_table_name}").collect()
                    snowpark_df.write.mode("overwrite").save_as_table(full_table_name)
                else:
                    # Subsequent batches: need to match existing schema
                    # Get existing columns and reorder/filter our data to match
                    try:
                        existing_df = session.table(full_table_name)
                        existing_cols = [f.name for f in existing_df.schema.fields]

                        # Select only existing columns in the right order
                        # Add missing columns as NULL literals
                        from snowflake.snowpark.functions import lit

                        select_exprs = []
                        for col in existing_cols:
                            if col in all_columns:
                                select_exprs.append(snowpark_df[col])
                            else:
                                select_exprs.append(lit(None).alias(col))

                        aligned_df = snowpark_df.select(*select_exprs)
                        aligned_df.write.mode("append").save_as_table(full_table_name)
                    except Exception:
                        # If table doesn't exist, just create it
                        snowpark_df.write.mode("overwrite").save_as_table(
                            full_table_name
                        )

            except Exception as e:
                errors.append(f"{table_name}: {e!s}")

        return snowpark_destination

    def resource(self, name: str, write_disposition: str = "replace", **kwargs):
        """
        Decorator to create a dlt resource.

        This is a convenience wrapper around @dlt.resource that provides
        sensible defaults for use with this loader.

        Args:
            name: Name of the destination table
            write_disposition: "replace" (default) or "append"
            **kwargs: Additional arguments passed to dlt.resource

        Returns:
            dlt.resource decorator

        Example:
            @loader.resource(name="users", write_disposition="replace")
            def get_users():
                yield [{"id": 1, "name": "Alice"}]
        """
        return dlt.resource(name=name, write_disposition=write_disposition, **kwargs)

    def run(self, *resources) -> dict[str, Any]:
        """
        Run the dlt pipeline with the given resources.

        Args:
            *resources: One or more dlt resources to load

        Returns:
            Dictionary with load results including:
            - status: "success", "partial" (some errors), or "error"
            - database: Target database name
            - schema: Target schema name
            - tables_loaded: Dict of table names to row counts
            - errors: List of error messages (if any)
            - started_at: Pipeline start timestamp
            - finished_at: Pipeline finish timestamp

        Example:
            result = loader.run(customers_resource(), orders_resource())
            print(f"Loaded {result['tables_loaded']}")
        """
        # Reset state for new run
        self.loaded_tables = {}
        self.errors = []

        try:
            pipeline = dlt.pipeline(
                pipeline_name=self.pipeline_name,
                destination=self._create_destination(),
                pipelines_dir="/tmp/dlt_pipelines",
            )

            load_info = pipeline.run(list(resources))

            return {
                "status": "success" if not self.errors else "partial",
                "database": self._db,
                "schema": self._schema,
                "tables_loaded": self.loaded_tables,
                "total_rows": sum(self.loaded_tables.values()),
                "errors": self.errors or None,
                "load_info": {
                    "pipeline_name": self.pipeline_name,
                    "started_at": str(load_info.started_at)
                    if load_info.started_at
                    else None,
                    "finished_at": str(load_info.finished_at)
                    if load_info.finished_at
                    else None,
                },
            }

        except Exception as e:
            return {
                "status": "error",
                "database": self._db,
                "schema": self._schema,
                "tables_loaded": self.loaded_tables,
                "total_rows": sum(self.loaded_tables.values()),
                "errors": [*self.errors, str(e)],
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc(),
            }

    def run_json(self, *resources) -> str:
        """
        Run the dlt pipeline and return JSON result.

        This is a convenience method that wraps run() and returns
        a formatted JSON string, suitable for returning directly
        from a stored procedure.

        Args:
            *resources: One or more dlt resources to load

        Returns:
            JSON string with load results

        Example:
            return loader.run_json(my_resource())
        """
        return json.dumps(self.run(*resources), indent=2, default=str)

    def verify_table(self, table_name: str, limit: int = 5) -> dict[str, Any]:
        """
        Verify loaded data by querying the table.

        Args:
            table_name: Name of the table to verify
            limit: Maximum number of sample rows to return

        Returns:
            Dictionary with row count and sample data
        """
        full_table_name = f"{self._db}.{self._schema}.{table_name.upper()}"

        try:
            # Use Snowpark DataFrame APIs instead of raw SQL
            df = self.session.table(full_table_name)

            # Get row count using DataFrame.count()
            row_count = df.count()

            # Get sample data using DataFrame.limit().collect()
            sample_rows = df.limit(limit).collect()
            sample_data = [row.as_dict() for row in sample_rows]

            return {
                "table": full_table_name,
                "row_count": row_count,
                "sample_data": sample_data,
            }

        except Exception as e:
            return {"table": full_table_name, "error": str(e)}
