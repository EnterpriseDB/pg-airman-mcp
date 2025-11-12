# ruff: noqa: B008
import argparse
import asyncio
import logging
import os
import signal
import sys
import threading
from enum import Enum
from typing import Any, Literal

import mcp.types as types
from mcp.server.fastmcp import FastMCP
from pydantic import Field, validate_call

from pg_airman_mcp.index.dta_calc import DatabaseTuningAdvisor

from .artifacts import ErrorResult, ExplainPlanArtifact
from .database_health import DatabaseHealthTool, HealthType
from .explain import ExplainPlanTool
from .index.index_opt_base import MAX_NUM_INDEX_TUNING_QUERIES
from .index.llm_opt import LLMOptimizerTool
from .index.presentation import TextPresentation
from .sql import (
    DbConnPool,
    SafeSqlDriver,
    SqlDriver,
    check_hypopg_installation_status,
    execute_comment_on,
    obfuscate_password,
)
from .top_queries import TopQueriesCalc

# Initialize FastMCP with default settings
mcp = FastMCP("pg-airman-mcp")

# Constants
PG_STAT_STATEMENTS = "pg_stat_statements"
HYPOPG_EXTENSION = "hypopg"

ResponseType = list[types.TextContent | types.ImageContent | types.EmbeddedResource]

logger = logging.getLogger(__name__)


class AccessMode(str, Enum):
    """SQL access modes for the server."""

    UNRESTRICTED = "unrestricted"  # Unrestricted access
    RESTRICTED = "restricted"  # Read-only with safety features


# Global variables
db_connection = DbConnPool()
current_access_mode = AccessMode.UNRESTRICTED
shutdown_in_progress = False
is_stdio_transport = False
shutdown_event = threading.Event()


async def get_sql_driver() -> SqlDriver | SafeSqlDriver:
    """Get the appropriate SQL driver based on the current access mode."""
    base_driver = SqlDriver(conn=db_connection)

    if current_access_mode == AccessMode.RESTRICTED:
        logger.debug("Using SafeSqlDriver with restrictions (RESTRICTED mode)")
        return SafeSqlDriver(sql_driver=base_driver, timeout=30)  # 30 second timeout
    else:
        logger.debug("Using unrestricted SqlDriver (UNRESTRICTED mode)")
        return base_driver


def format_text_response(text: Any) -> ResponseType:
    """Format a text response."""
    return [types.TextContent(type="text", text=str(text))]


def format_error_response(error: str) -> ResponseType:
    """Format an error response."""
    return format_text_response(f"Error: {error}")


@mcp.tool(description="List all schemas in the database")
async def list_schemas() -> ResponseType:
    """List all schemas in the database."""
    try:
        sql_driver = await get_sql_driver()
        rows = await sql_driver.execute_query(
            """
            SELECT
                schema_name,
                schema_owner,
                CASE
                    WHEN schema_name LIKE 'pg_%' THEN 'System Schema'
                    WHEN schema_name = 'information_schema' THEN 'System Information Schema'
                    ELSE 'User Schema'
                END as schema_type
            FROM information_schema.schemata
            ORDER BY schema_type, schema_name
            """  # noqa: E501
        )
        schemas = [row.cells for row in rows] if rows else []
        return format_text_response(schemas)
    except Exception as e:
        logger.error(f"Error listing schemas: {e}")
        return format_error_response(str(e))


@mcp.tool(description="List objects in a schema with comments")
async def list_objects(
    schema_name: str = Field(description="Schema name"),
    object_type: str = Field(
        description="Object type: 'table', 'view', 'sequence','function', "
        "'stored procedure', or 'extension'",
        default="table",
    ),
) -> ResponseType:
    """List objects of a given type in a schema, including object-level comments."""
    try:
        sql_driver = await get_sql_driver()

        if object_type in ("table", "view"):
            # Use pg_catalog so we can fetch comments via obj_description(pg_class.oid, 'pg_class')  # noqa: E501
            relkinds = (
                ("'r'",) if object_type == "table" else ("'v'",)
            )  # 'r' table, 'v' view
            rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                f"""
                SELECT
                  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' ELSE c.relkind::text END AS object_type,
                  n.nspname AS table_schema,
                  c.relname AS table_name,
                  d.description AS comment
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                LEFT JOIN pg_catalog.pg_description d ON d.objoid = c.oid AND d.objsubid = 0
                WHERE n.nspname = {{}} AND c.relkind IN ({", ".join(relkinds)})
                ORDER BY c.relname
                """,  # noqa: E501
                [schema_name],
            )
            objects = (
                [
                    {
                        "schema": row.cells["table_schema"],
                        "name": row.cells["table_name"],
                        "type": row.cells["object_type"],
                        "comment": row.cells["comment"],
                    }
                    for row in rows or []
                ]
                if rows
                else []
            )

        elif object_type == "sequence":
            rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT
                  'sequence' AS object_type,
                  n.nspname AS sequence_schema,
                  c.relname  AS sequence_name,
                  d.description AS comment
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                LEFT JOIN pg_catalog.pg_description d ON d.objoid = c.oid AND d.objsubid = 0
                WHERE n.nspname = {} AND c.relkind = 'S'
                ORDER BY c.relname
                """,  # noqa: E501
                [schema_name],
            )
            objects = (
                [
                    {
                        "schema": row.cells["sequence_schema"],
                        "name": row.cells["sequence_name"],
                        "type": row.cells["object_type"],
                        "comment": row.cells["comment"],
                    }
                    for row in rows
                ]
                if rows
                else []
            )

        elif object_type == "extension":
            # Extensions are not schema-specific
            rows = await sql_driver.execute_query(
                """
                SELECT
                  e.extname AS name,
                  e.extversion AS version,
                  e.extrelocatable AS relocatable,
                  d.description AS comment
                FROM pg_catalog.pg_extension e
                LEFT JOIN pg_catalog.pg_description d ON d.objoid = e.oid AND d.objsubid = 0
                ORDER BY e.extname
                """  # noqa: E501
            )
            objects = (
                [
                    {
                        "name": row.cells["name"],
                        "version": row.cells["version"],
                        "relocatable": row.cells["relocatable"],
                        "comment": row.cells["comment"],
                    }
                    for row in rows
                ]
                if rows
                else []
            )
        elif object_type in ("function", "procedure"):
            # prokind: 'f' = function, 'p' = procedure. Avoid obj_description(); use pg_description join.  # noqa: E501
            prokind = "f" if object_type == "function" else "p"
            rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT
                  CASE p.prokind WHEN 'p' THEN 'procedure' ELSE 'function' END AS object_type,
                  n.nspname AS routine_schema,
                  p.proname AS routine_name,        -- keep simple name to avoid catalog functions
                  d.description AS comment
                FROM pg_catalog.pg_proc p
                JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
                LEFT JOIN pg_catalog.pg_description d ON d.objoid = p.oid AND d.objsubid = 0
                WHERE n.nspname = {} AND p.prokind = {}
                ORDER BY routine_name
                """,  # noqa: E501
                [schema_name, prokind],
            )
            objects = (
                [
                    {
                        "schema": row.cells["routine_schema"],
                        "name": row.cells["routine_name"],
                        "type": row.cells["object_type"],
                        "comment": row.cells["comment"],
                    }
                    for row in rows
                ]
                if rows
                else []
            )
        else:
            return format_error_response(f"Unsupported object type: {object_type}")

        return format_text_response(objects)
    except Exception as e:
        logger.error(f"Error listing objects: {e}")
        return format_error_response(str(e))


@mcp.tool(description="Show detailed information about a database object with comments")
async def get_object_details(
    schema_name: str = Field(description="Schema name"),
    object_name: str = Field(description="Object name"),
    object_type: str = Field(
        description="Object type: 'table', 'view', 'sequence', or 'extension'",
        default="table",
    ),
) -> ResponseType:
    """Get detailed information about a database object."""
    try:
        sql_driver = await get_sql_driver()

        if object_type in ("table", "view"):
            # Get table/view details
            obj_comment_rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT d.description AS comment
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                LEFT JOIN pg_catalog.pg_description d ON d.objoid = c.oid AND d.objsubid = 0
                WHERE n.nspname = {} AND c.relname = {}
                """,  # noqa: E501
                [schema_name, object_name],
            )
            object_comment = (
                obj_comment_rows[0].cells["comment"] if obj_comment_rows else None
            )

            # Get columns
            col_rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = {} AND table_name = {}
                ORDER BY ordinal_position
                """,
                [schema_name, object_name],
            )
            columns = (
                [
                    {
                        "column": r.cells["column_name"],
                        "data_type": r.cells["data_type"],
                        "is_nullable": r.cells["is_nullable"],
                        "default": r.cells["column_default"],
                        "comment": None,
                    }
                    for r in col_rows
                ]
                if col_rows
                else []
            )
            col_cmt_rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT a.attname AS column_name, d.description AS comment
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_catalog.pg_attribute a
                  ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
                LEFT JOIN pg_catalog.pg_description d
                  ON d.objoid = c.oid AND d.objsubid = a.attnum
                WHERE n.nspname = {} AND c.relname = {}
                ORDER BY a.attnum
                """,
                [schema_name, object_name],
            )
            # Map comments by column name and merge
            col_comments = {
                r.cells["column_name"]: r.cells["comment"] for r in col_cmt_rows or []
            }
            for col in columns:
                col["comment"] = col_comments.get(col["column"])

            # Get constraints
            con_rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT tc.constraint_name, tc.constraint_type, kcu.column_name
                FROM information_schema.table_constraints AS tc
                LEFT JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                WHERE tc.table_schema = {} AND tc.table_name = {}
                """,
                [schema_name, object_name],
            )

            constraints = {}
            if con_rows:
                for row in con_rows:
                    cname = row.cells["constraint_name"]
                    ctype = row.cells["constraint_type"]
                    col = row.cells["column_name"]

                    if cname not in constraints:
                        constraints[cname] = {"type": ctype, "columns": []}
                    if col:
                        constraints[cname]["columns"].append(col)

            constraints_list = [
                {"name": name, **data} for name, data in constraints.items()
            ]

            # Get indexes
            idx_rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE schemaname = {} AND tablename = {}
                """,
                [schema_name, object_name],
            )

            indexes = (
                [
                    {"name": r.cells["indexname"], "definition": r.cells["indexdef"]}
                    for r in idx_rows
                ]
                if idx_rows
                else []
            )

            result = {
                "basic": {
                    "schema": schema_name,
                    "name": object_name,
                    "type": object_type,
                    "comment": object_comment,
                },
                "columns": columns,
                "constraints": constraints_list,
                "indexes": indexes,
            }

        elif object_type == "sequence":
            rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT sequence_schema, sequence_name, data_type, start_value, increment
                FROM information_schema.sequences
                WHERE sequence_schema = {} AND sequence_name = {}
                """,
                [schema_name, object_name],
            )

            if rows and rows[0]:
                row = rows[0]
                cmt_rows = await SafeSqlDriver.execute_param_query(
                    sql_driver,
                    """
                    SELECT d.description AS comment
                    FROM pg_catalog.pg_class c
                    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    LEFT JOIN pg_catalog.pg_description d ON d.objoid = c.oid AND d.objsubid = 0
                    WHERE n.nspname = {} AND c.relname = {}
                      AND c.relkind = 'S'
                    """,  # noqa: E501
                    [schema_name, object_name],
                )
                seq_comment = cmt_rows[0].cells["comment"] if cmt_rows else None
                result = {
                    "schema": row.cells["sequence_schema"],
                    "name": row.cells["sequence_name"],
                    "data_type": row.cells["data_type"],
                    "start_value": row.cells["start_value"],
                    "increment": row.cells["increment"],
                    "comment": seq_comment,
                }
            else:
                result = {}

        elif object_type == "extension":
            rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT e.extname AS name, e.extversion AS version, e.extrelocatable AS relocatable, d.description AS comment
                FROM pg_catalog.pg_extension e
                LEFT JOIN pg_catalog.pg_description d ON d.objoid = e.oid AND d.objsubid = 0
                WHERE e.extname = {}
                """,  # noqa: E501
                [object_name],
            )

            if rows and rows[0]:
                row = rows[0]
                result = {
                    "name": row.cells["name"],
                    "version": row.cells["version"],
                    "relocatable": row.cells["relocatable"],
                    "comment": row.cells["comment"],
                }
            else:
                result = {}
        elif object_type in ("function", "procedure"):
            # Routine comment via pg_description; avoid catalog functions
            # to keep validator happy
            prokind = "p" if object_type == "procedure" else "f"
            rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT
                  n.nspname AS routine_schema,
                  p.proname AS routine_name,
                  p.prokind AS kind,
                  d.description AS comment
                FROM pg_catalog.pg_proc p
                JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
                LEFT JOIN pg_catalog.pg_description d ON d.objoid = p.oid AND d.objsubid = 0
                WHERE n.nspname = {} AND p.proname = {} AND p.prokind = {}
                ORDER BY routine_name
                """,  # noqa: E501
                [schema_name, object_name, prokind],
            )
            result = [
                {
                    "schema": r.cells["routine_schema"],
                    "name": r.cells["routine_name"],
                    "kind": r.cells["kind"],
                    "comment": r.cells["comment"],
                }
                for r in rows or []
            ]
        else:
            return format_error_response(f"Unsupported object type: {object_type}")

        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error getting object details: {e}")
        return format_error_response(str(e))


@mcp.tool(
    description="Explains the execution plan for a SQL query, showing how the database "
    "will execute it and provides detailed cost estimates."
)
async def explain_query(
    sql: str = Field(description="SQL query to explain"),
    analyze: bool = Field(
        description="When True, actually runs the query to show real execution "
        "statistics instead of estimates. "
        "Takes longer but provides more accurate information.",
        default=False,
    ),
    hypothetical_indexes: list[dict[str, Any]] = Field(
        description="""A list of hypothetical indexes to simulate. Each index must be a"
        " dictionary with these keys:
    - 'table': The table name to add the index to (e.g., 'users')
    - 'columns': List of column names to include in the index (e.g., ['email'] or "
    "['last_name', 'first_name'])
    - 'using': Optional index method (default: 'btree', other options include 'hash', "
    "'gist', etc.)

Examples: [
    {"table": "users", "columns": ["email"], "using": "btree"},
    {"table": "orders", "columns": ["user_id", "created_at"]}
]
If there is no hypothetical index, you can pass an empty list.""",
        default=[],
    ),
) -> ResponseType:
    """
    Explains the execution plan for a SQL query.

    Args:
        sql: The SQL query to explain
        analyze: When True, actually runs the query for real statistics
        hypothetical_indexes: Optional list of indexes to simulate
    """
    try:
        sql_driver = await get_sql_driver()
        explain_tool = ExplainPlanTool(sql_driver=sql_driver)
        result: ExplainPlanArtifact | ErrorResult | None = None

        # If hypothetical indexes are specified, check for HypoPG extension
        if hypothetical_indexes and len(hypothetical_indexes) > 0:
            if analyze:
                return format_error_response(
                    "Cannot use analyze and hypothetical indexes together"
                )
            try:
                # Use the common utility function to check if hypopg is installed
                (
                    is_hypopg_installed,
                    hypopg_message,
                ) = await check_hypopg_installation_status(sql_driver)

                # If hypopg is not installed, return the message
                if not is_hypopg_installed:
                    return format_text_response(hypopg_message)

                # HypoPG is installed, proceed with explaining with hypothetical indexes
                result = await explain_tool.explain_with_hypothetical_indexes(
                    sql, hypothetical_indexes
                )
            except Exception:
                raise  # Re-raise the original exception
        elif analyze:
            try:
                # Use EXPLAIN ANALYZE
                result = await explain_tool.explain_analyze(sql)
            except Exception:
                raise  # Re-raise the original exception
        else:
            try:
                # Use basic EXPLAIN
                result = await explain_tool.explain(sql)
            except Exception:
                raise  # Re-raise the original exception

        if result and isinstance(result, ExplainPlanArtifact):
            return format_text_response(result.to_text())
        else:
            error_message = "Error processing explain plan"
            if isinstance(result, ErrorResult):
                error_message = result.to_text()
            return format_error_response(error_message)
    except Exception as e:
        logger.error(f"Error explaining query: {e}")
        return format_error_response(str(e))


# Query function declaration without the decorator - we'll add it dynamically
# based on access mode
async def execute_sql(
    sql: str = Field(description="SQL to run", default="all"),
) -> ResponseType:
    """Executes a SQL query against the database."""
    try:
        sql_driver = await get_sql_driver()
        rows = await sql_driver.execute_query(sql)  # type: ignore
        if rows is None:
            return format_text_response("No results")
        return format_text_response(list([r.cells for r in rows]))
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        return format_error_response(str(e))


@mcp.tool(
    description="Analyze frequently executed queries in the database and recommend "
    "optimal indexes"
)
@validate_call
async def analyze_workload_indexes(
    max_index_size_mb: int = Field(description="Max index size in MB", default=10000),
    method: Literal["dta", "llm"] = Field(
        description="Method to use for analysis", default="dta"
    ),
) -> ResponseType:
    """
    Analyze frequently executed queries in the database and recommend optimal indexes.
    """
    try:
        sql_driver = await get_sql_driver()
        if method == "dta":
            index_tuning = DatabaseTuningAdvisor(sql_driver)
        else:
            index_tuning = LLMOptimizerTool(sql_driver)
        dta_tool = TextPresentation(sql_driver, index_tuning)
        result = await dta_tool.analyze_workload(max_index_size_mb=max_index_size_mb)
        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error analyzing workload: {e}")
        return format_error_response(str(e))


@mcp.tool(
    description="Analyze a list of (up to 10) SQL queries and recommend optimal indexes"
)
@validate_call
async def analyze_query_indexes(
    queries: list[str] = Field(description="List of Query strings to analyze"),
    max_index_size_mb: int = Field(description="Max index size in MB", default=10000),
    method: Literal["dta", "llm"] = Field(
        description="Method to use for analysis", default="dta"
    ),
) -> ResponseType:
    """Analyze a list of SQL queries and recommend optimal indexes."""
    if len(queries) == 0:
        return format_error_response(
            "Please provide a non-empty list of queries to analyze."
        )
    if len(queries) > MAX_NUM_INDEX_TUNING_QUERIES:
        return format_error_response(
            f"Please provide a list of up to {MAX_NUM_INDEX_TUNING_QUERIES} queries "
            "to analyze."
        )

    try:
        sql_driver = await get_sql_driver()
        if method == "dta":
            index_tuning = DatabaseTuningAdvisor(sql_driver)
        else:
            index_tuning = LLMOptimizerTool(sql_driver)
        dta_tool = TextPresentation(sql_driver, index_tuning)
        result = await dta_tool.analyze_queries(
            queries=queries, max_index_size_mb=max_index_size_mb
        )
        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error analyzing queries: {e}")
        return format_error_response(str(e))


@mcp.tool(
    description="Analyzes database health. Here are the available health checks:\n"
    "- index - checks for invalid, duplicate, and bloated indexes\n"
    "- connection - checks the number of connection and their utilization\n"
    "- vacuum - checks vacuum health for transaction id wraparound\n"
    "- sequence - checks sequences at risk of exceeding their maximum value\n"
    "- replication - checks replication health including lag and slots\n"
    "- buffer - checks for buffer cache hit rates for indexes and tables\n"
    "- constraint - checks for invalid constraints\n"
    "- all - runs all checks\n"
    "You can optionally specify a single health check or a comma-separated list of "
    "health checks. The default is 'all' checks."
)
async def analyze_db_health(
    health_type: str = Field(
        description="Optional. Valid values are: "
        f"{', '.join(sorted([t.value for t in HealthType]))}.",
        default="all",
    ),
) -> ResponseType:
    """Analyze database health for specified components.

    Args:
        health_type: Comma-separated list of health check types to perform.
                    Valid values: index, connection, vacuum, sequence, replication,
                    buffer, constraint, all
    """
    health_tool = DatabaseHealthTool(await get_sql_driver())
    result = await health_tool.health(health_type=health_type)
    return format_text_response(result)


@mcp.tool(
    name="get_top_queries",
    description="Reports the slowest or most resource-intensive queries using data "
    "from the '{PG_STAT_STATEMENTS}' extension.",
)
async def get_top_queries(
    sort_by: str = Field(
        description="Ranking criteria: 'total_time' for total execution time or "
        "'mean_time' for mean execution time per call, or 'resources' "
        "for resource-intensive queries",
        default="resources",
    ),
    limit: int = Field(
        description="Number of queries to return when ranking based on mean_time or "
        "total_time",
        default=10,
    ),
) -> ResponseType:
    try:
        sql_driver = await get_sql_driver()
        top_queries_tool = TopQueriesCalc(sql_driver=sql_driver)

        if sort_by == "resources":
            result = await top_queries_tool.get_top_resource_queries()
            return format_text_response(result)
        elif sort_by == "mean_time" or sort_by == "total_time":
            # Map the sort_by values to what get_top_queries_by_time expects
            result = await top_queries_tool.get_top_queries_by_time(
                limit=limit, sort_by="mean" if sort_by == "mean_time" else "total"
            )
        else:
            return format_error_response(
                "Invalid sort criteria. Please use 'resources' or 'mean_time' or "
                "'total_time'."
            )
        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error getting slow queries: {e}")
        return format_error_response(str(e))


@mcp.tool(
    name="add_comment_to_object", description="Adds a comment to a database object."
)
async def add_comment_to_object(
    schema_name: str = Field(description="Schema name"),
    object_type: str = Field(
        description="Object type: 'table', 'view', or 'column'",
        default="table",
    ),
    object_name: str = Field(description="Object name"),
    comment: str = Field(description="Comment text"),
    column_name: str | None = Field(
        description="Column name (if object_type is 'column')", default=None
    ),
) -> ResponseType:
    """Add a comment to a database object."""
    try:
        allowed_object_types = {"table": "TABLE", "view": "VIEW", "column": "COLUMN"}
        normalized_type = object_type.lower()
        kind = allowed_object_types.get(normalized_type)
        if not kind:
            return format_error_response(
                "Unsupported object type. Use 'table', 'view', or 'column'."
            )

        if normalized_type in ("table", "view"):
            if not schema_name:
                return format_error_response("Schema name is required for table/view.")
            parts = [schema_name, object_name]
        else:  # column
            if not schema_name or not object_name or not column_name:
                return format_error_response(
                    "Schema, object, and column names are required for column comments."
                )
            parts = [schema_name, object_name, column_name]

        sql_driver = await get_sql_driver()
        await execute_comment_on(sql_driver, kind, parts, comment)
        return format_text_response(
            f"Successfully added comment to {normalized_type} '{object_name}'."
        )
    except Exception as e:
        logger.error(f"Error executing comment statement: {e}")
        return format_error_response(str(e))


def signal_handler(signal, _) -> None:
    """
    Method for handling incoming OS signals for graceful shutdown
    or immediate exit.

    - Logs the received signal.
    - If running with stdio transport, exits the process immediately.
    - Otherwise, triggers a graceful shutdown by setting the shutdown event.
    """
    logger.info(f"Received signal {signal}")
    if is_stdio_transport:
        logger.info("Stdio transport detected - using sys.exit()")
        sys.exit(0)
    else:
        logger.info("Non-stdio transport - using graceful shutdown")
        shutdown_event.set()


async def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Pg Airman MCP Server")
    parser.add_argument("database_url", help="Database connection URL", nargs="?")
    parser.add_argument(
        "--access-mode",
        type=str,
        choices=[mode.value for mode in AccessMode],
        default=AccessMode.UNRESTRICTED.value,
        help="Set SQL access mode: unrestricted (unrestricted) or restricted "
        "(read-only with protections)",
    )
    parser.add_argument(
        "--transport",
        type=str,
        choices=["stdio", "sse", "streamable-http"],
        default="stdio",
        help="Select MCP transport: stdio (default), sse or streamable-http",
    )
    parser.add_argument(
        "--sse-host",
        type=str,
        default="localhost",
        help="Host to bind SSE server to (default: localhost)",
    )
    parser.add_argument(
        "--sse-port",
        type=int,
        default=8000,
        help="Port for SSE server (default: 8000)",
    )
    parser.add_argument(
        "--streamable-http-host",
        type=str,
        default="localhost",
        help="Host to bind streamable http server to (default: localhost)",
    )
    parser.add_argument(
        "--streamable-http-port",
        type=int,
        default=8001,
        help="Port for streamable http server (default: 8001)",
    )

    args = parser.parse_args()

    # Store the access mode in the global variable
    global current_access_mode
    current_access_mode = AccessMode(args.access_mode)

    # Add the query tool with a description appropriate to the access mode
    if current_access_mode == AccessMode.UNRESTRICTED:
        mcp.add_tool(execute_sql, description="Execute any SQL query")
    else:
        mcp.add_tool(execute_sql, description="Execute a read-only SQL query")

    logger.info(f"Starting Pg Airman MCP Server in {current_access_mode.upper()} mode")

    # Get database URL from environment variable or command line
    database_url = os.environ.get("DATABASE_URI", args.database_url)

    if not database_url:
        raise ValueError(
            "Error: No database URL provided. Please specify via 'DATABASE_URI' "
            "environment variable or command-line argument.",
        )

    # Initialize database connection pool
    try:
        await db_connection.pool_connect(database_url)
        logger.info(
            "Successfully connected to database and initialized connection pool"
        )
    except Exception as e:
        logger.warning(
            f"Could not connect to database: {obfuscate_password(str(e))}",
        )
        logger.warning(
            "The MCP server will start but database operations will fail until a valid "
            "connection is established.",
        )

    # Set up proper shutdown handling
    if sys.platform != "win32":
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    else:
        # On Windows, only SIGINT can be handled; SIGTERM is not supported.
        signal.signal(signal.SIGINT, signal_handler)
        logger.warning(
            "Limited signal handling on Windows: only SIGINT is handled, SIGTERM "
            "is not supported."
        )
    global shutdown_in_progress, is_stdio_transport
    is_stdio_transport = args.transport == "stdio"
    try:
        logger.info("Server starting...")
        while not shutdown_event.is_set():
            # Run the server with the selected transport (always async)
            if args.transport == "stdio":
                await mcp.run_stdio_async()
            elif args.transport == "sse":
                mcp.settings.host = args.sse_host
                mcp.settings.port = args.sse_port
                await mcp.run_sse_async()
            elif args.transport == "streamable-http":
                mcp.settings.host = args.streamable_http_host
                mcp.settings.port = args.streamable_http_port
                await mcp.run_streamable_http_async()
            await asyncio.sleep(0.1)
        logger.info("Shutdown requested, cleaning up...")
        await shutdown()
    except (asyncio.CancelledError, KeyboardInterrupt) as e:
        if isinstance(e, asyncio.CancelledError):
            logger.info("Server task cancelled")
        else:
            logger.info("Received keyboard interrupt")
        await handle_transport_exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        await handle_transport_exit(1)
    finally:
        logger.info("Server stopped")
        # Graceful exit for MCP servers
        sys.exit(0)


async def handle_transport_exit(exit_code: int = 0):
    """Handle transport exit by triggering shutdown."""
    if is_stdio_transport:
        sys.exit(exit_code)
    else:
        await shutdown()


async def shutdown(sig=None):
    """Clean shutdown of the server."""

    if sig:
        logger.info(f"Received exit signal {sig.name}")

    # Close database connections
    try:
        logger.info("Closing database connection...")
        await db_connection.close()
        logger.info("Closed database connections")
    except Exception as e:
        logger.error(f"Error closing database connections: {e}")

    # Exit with appropriate status code
    logger.info("Shutdown complete")
