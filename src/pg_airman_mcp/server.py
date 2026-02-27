# ruff: noqa: B008
# B008: Do not perform function call in argument defaults
# Disabled because Pydantic Field() calls are required in function signatures
# for tool registration
"""
Pg Airman MCP Server - PostgreSQL Database Management via Model Context
Protocol.

This module implements a Model Context Protocol (MCP) server that provides
AI assistants with tools for PostgreSQL database management, query
optimization, and health monitoring.

Architecture Overview
--------------------
The server is built on FastMCP and provides 10 database management tools:
  - Schema introspection (list_schemas, list_objects, get_object_details)
  - Query optimization (explain_query, analyze_workload_indexes, analyze_query_indexes)
  - Database health monitoring (analyze_db_health, get_top_queries)
  - Metadata management (add_comment_to_object)
  - SQL execution (execute_sql - access mode dependent)

Configuration System
-------------------
Settings are loaded via Pydantic BaseSettings with a three-tier priority system:
  1. Command-line arguments (highest priority)
  2. Environment variables with AIRMAN_MCP_ prefix
  3. Default values (lowest priority)

Access Modes
-----------
- UNRESTRICTED: Full SQL execution capabilities (default)
- RESTRICTED: Read-only queries with 30-second timeout via SafeSqlDriver

Transport Options
----------------
- stdio: Standard input/output communication (default)
- sse: Server-Sent Events over HTTP
- streamable-http: Streamable HTTP transport

Authentication
-------------
Optional OAuth 2.0 authentication with token introspection:
  - Auth wiring is in auth_config.py (AuthContext, create_auth_context)
  - Token verification via token_verifier.py (any RFC 7662 endpoint)
  - Supports RFC 8707 resource validation
  - All tools protected when auth is enabled
  - Requires AIRMAN_MCP_SERVER_URL for stdio transport with auth

Global State
-----------
Module-level globals used for server coordination:
  - db_connection: PostgreSQL connection pool (DbConnPool)
  - current_access_mode: Active access mode (AccessMode.UNRESTRICTED/RESTRICTED)
  - is_stdio_transport: Flag for transport-specific shutdown behavior
  - shutdown_event: Threading event for graceful shutdown coordination
  - mcp: FastMCP server instance (created in main())

Entry Point
----------
Run with: python -m pg_airman_mcp <database_url> [options]
See main() function for complete CLI argument documentation.
"""

import argparse
import asyncio
import logging
import signal
import sys
import threading
from enum import Enum
from typing import Any, Literal

import mcp.types as types
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import Field, field_validator, validate_call
from pydantic_settings import BaseSettings, SettingsConfigDict

from pg_airman_mcp.index.dta_calc import DatabaseTuningAdvisor

from .artifacts import ErrorResult, ExplainPlanArtifact
from .auth_config import (
    AuthContext,
    add_auth_cli_args,
    apply_auth_cli_overrides,
    create_auth_context,
)
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

# MCP server instance - created in main() with or without authentication
# All tools are registered dynamically in create_mcp_server()
mcp: FastMCP

# Constants
PG_STAT_STATEMENTS = "pg_stat_statements"
HYPOPG_EXTENSION = "hypopg"

ResponseType = list[types.TextContent | types.ImageContent | types.EmbeddedResource]

logger = logging.getLogger(__name__)


class AccessMode(str, Enum):
    """SQL access modes for the server."""

    UNRESTRICTED = "unrestricted"  # Unrestricted access
    RESTRICTED = "restricted"  # Read-only with safety features


class ServerSettings(BaseSettings):
    """
    Configuration settings for the Pg Airman MCP Server.

    All settings can be configured via:
    1. Command-line arguments (takes precedence)
    2. Environment variables with AIRMAN_MCP_ prefix
    3. Default values (lowest priority)

    Environment Variables:
        AIRMAN_MCP_DATABASE_URL: PostgreSQL connection string
        AIRMAN_MCP_ACCESS_MODE: SQL access mode (unrestricted or restricted)
        AIRMAN_MCP_TRANSPORT: Transport type (stdio, sse, streamable-http)
        AIRMAN_MCP_SSE_HOST: SSE server host (default: localhost)
        AIRMAN_MCP_SSE_PORT: SSE server port (default: 8000)
        AIRMAN_MCP_STREAMABLE_HTTP_HOST: Streamable HTTP host (default: localhost)
        AIRMAN_MCP_STREAMABLE_HTTP_PORT: Streamable HTTP port (default: 8001)
        AIRMAN_MCP_AUTH_ENABLED: Enable authentication (true/false)
        AIRMAN_MCP_AUTH_SERVER_URL: Authorization server URL
        AIRMAN_MCP_AUTH_INTROSPECTION_ENDPOINT: Token introspection endpoint
        AIRMAN_MCP_AUTH_REQUIRED_SCOPES: Comma-separated OAuth scopes
        AIRMAN_MCP_AUTH_VALIDATE_RESOURCE: Enable RFC 8707 validation (true/false)
        AIRMAN_MCP_SERVER_URL: This server's URL (required for stdio with auth)
        AIRMAN_MCP_DNS_REBINDING_PROTECTION: DNS rebinding protection
        AIRMAN_MCP_ALLOWED_HOSTS: Allowed Host header values (CSV)
        AIRMAN_MCP_ALLOWED_ORIGINS: Allowed Origin header values (CSV)

    Example Usage:
        # Via environment variables
        export AIRMAN_MCP_DATABASE_URL="postgresql://user:pass@localhost/db"
        export AIRMAN_MCP_ACCESS_MODE="restricted"
        export AIRMAN_MCP_AUTH_ENABLED="true"
        python -m pg_airman_mcp

        # Via CLI (overrides environment)
        python -m pg_airman_mcp "postgresql://..." --access-mode restricted

        # Programmatically
        settings = ServerSettings()
        print(settings.database_url)
    """

    model_config = SettingsConfigDict(env_prefix="AIRMAN_MCP_", case_sensitive=False)

    # Database configuration
    database_url: str | None = Field(
        default=None,
        description="PostgreSQL connection URL",
    )

    # Access mode
    access_mode: str = Field(
        default="unrestricted",
        description="SQL access mode: unrestricted or restricted",
    )

    # Transport configuration
    transport: str = Field(
        default="stdio",
        description="MCP transport: stdio, sse, or streamable-http",
    )
    sse_host: str = Field(default="localhost", description="SSE server host")
    sse_port: int = Field(default=8000, description="SSE server port")
    streamable_http_host: str = Field(
        default="localhost", description="Streamable HTTP server host"
    )
    streamable_http_port: int = Field(
        default=8001, description="Streamable HTTP server port"
    )

    # Authentication configuration
    auth_enabled: bool = Field(
        default=False, description="Enable OAuth authentication for all tools"
    )
    auth_server_url: str = Field(
        default="http://localhost:9000",
        description="Authorization server base URL",
    )
    auth_introspection_endpoint: str | None = Field(
        default=None,
        description="Token introspection endpoint URL "
        "(default: {auth-server-url}/introspect)",
    )
    auth_required_scopes: str = Field(
        default="mcp:postgres:access",
        description="Comma-separated list of required OAuth scopes",
    )
    auth_validate_resource: bool = Field(
        default=False, description="Enable RFC 8707 resource validation"
    )
    auth_introspection_client_id: str | None = Field(
        default=None,
        description="Client ID for authenticating introspection requests",
    )
    auth_introspection_client_secret: str | None = Field(
        default=None,
        description="Client secret for authenticating introspection requests",
    )
    server_url: str | None = Field(
        default=None,
        description="This MCP server's URL (required for stdio transport with auth)",
    )

    # DNS rebinding protection
    dns_rebinding_protection: bool = Field(
        default=False,
        description="Enable DNS rebinding protection for HTTP transports",
    )
    allowed_hosts: str = Field(
        default="",
        description="Comma-separated allowed Host header values "
        "(e.g. 'myservice:*,localhost:*')",
    )
    allowed_origins: str = Field(
        default="",
        description="Comma-separated allowed Origin header values",
    )

    @field_validator("access_mode")
    @classmethod
    def validate_access_mode(cls, v: str) -> str:
        """Validate access mode is one of the allowed values."""
        allowed = ["unrestricted", "restricted"]
        if v.lower() not in allowed:
            raise ValueError(f"access_mode must be one of {allowed}, got: {v}")
        return v.lower()

    @field_validator("transport")
    @classmethod
    def validate_transport(cls, v: str) -> str:
        """Validate transport is one of the allowed values."""
        allowed = ["stdio", "sse", "streamable-http"]
        if v.lower() not in allowed:
            raise ValueError(f"transport must be one of {allowed}, got: {v}")
        return v.lower()

    def get_required_scopes(self) -> list[str]:
        """Parse required scopes from comma-separated string."""
        return [s.strip() for s in self.auth_required_scopes.split(",") if s.strip()]

    def determine_server_url(self) -> str | None:
        """
        Determine the server URL based on transport and configuration.

        For stdio with auth, requires AIRMAN_MCP_SERVER_URL environment variable.
        For SSE/HTTP transports, auto-generates from host:port.
        """
        if self.server_url:
            return self.server_url

        if not self.auth_enabled:
            return None

        if self.transport == "stdio":
            # stdio with auth requires explicit server URL
            return None  # Will be validated later
        elif self.transport == "sse":
            host = "localhost" if self.sse_host in ("0.0.0.0", "::") else self.sse_host
            return f"http://{host}:{self.sse_port}"
        elif self.transport == "streamable-http":
            host = (
                "localhost"
                if self.streamable_http_host in ("0.0.0.0", "::")
                else self.streamable_http_host
            )
            return f"http://{host}:{self.streamable_http_port}"

        return None

    def build_transport_security(self) -> TransportSecuritySettings | None:
        """Build TransportSecuritySettings from server configuration.

        Returns:
            TransportSecuritySettings if explicit configuration is provided,
            None to let FastMCP apply its defaults.
        """
        hosts: list[str] = []
        origins: list[str] = []

        if self.allowed_hosts:
            hosts = [h.strip() for h in self.allowed_hosts.split(",") if h.strip()]
        if self.allowed_origins:
            origins = [o.strip() for o in self.allowed_origins.split(",") if o.strip()]

        # allowed_hosts/allowed_origins take priority: if set, the user
        # clearly wants protection enabled with a specific allowlist,
        # regardless of the dns_rebinding_protection flag.
        if hosts or origins:
            return TransportSecuritySettings(
                enable_dns_rebinding_protection=True,
                allowed_hosts=hosts,
                allowed_origins=origins,
            )

        if not self.dns_rebinding_protection:
            return TransportSecuritySettings(enable_dns_rebinding_protection=False)

        # dns_rebinding_protection=True but no allowlists — let FastMCP
        # apply its built-in defaults (localhost protection).
        return None


# Global variables
db_connection = DbConnPool()
_auth_context: AuthContext | None = None
current_access_mode = AccessMode.UNRESTRICTED
is_stdio_transport = False
shutdown_event = threading.Event()


def create_mcp_server(
    auth_context: AuthContext | None = None,
    transport_security: TransportSecuritySettings | None = None,
) -> FastMCP:
    """
    Create FastMCP server with optional OAuth authentication.

    Creates a new FastMCP instance (authenticated or not) and registers
    all tools. This provides a single, unified registration path for both
    authenticated and unauthenticated modes.

    Args:
        auth_context: Pre-configured auth objects from create_auth_context().
            If None, server runs without authentication.
        transport_security: DNS rebinding protection settings.
            If None, FastMCP applies its defaults (protection for localhost).

    Returns:
        Configured FastMCP server instance with all tools registered
    """
    server: FastMCP

    global _auth_context
    _auth_context = auth_context

    if auth_context is None:
        # Create unauthenticated server
        logger.info("Authentication DISABLED - all tools are unprotected")
        server = FastMCP("pg-airman-mcp", transport_security=transport_security)
    else:
        # Create authenticated FastMCP instance
        server = FastMCP(
            "pg-airman-mcp",
            token_verifier=auth_context.token_verifier,
            auth=auth_context.auth_settings,
            transport_security=transport_security,
        )

    # Register all tools on the server (works for both auth and no-auth)
    server.add_tool(list_schemas, description="List all schemas in the database")
    server.add_tool(list_objects, description="List objects in a schema with comments")
    server.add_tool(
        get_object_details,
        description="Show detailed information about a database object with comments",
    )
    server.add_tool(
        explain_query,
        description="Explains the execution plan for a SQL query, showing how "
        "the database will execute it and provides detailed cost estimates.",
    )
    server.add_tool(
        analyze_workload_indexes,
        description="Analyze frequently executed queries in the database and "
        "recommend optimal indexes",
    )
    server.add_tool(
        analyze_query_indexes,
        description="Analyze a list of (up to 10) SQL queries and recommend "
        "optimal indexes",
    )
    server.add_tool(
        analyze_db_health,
        description="Analyzes database health. Here are the available health "
        "checks:\n- index - checks for invalid, duplicate, and bloated "
        "indexes\n- connection - checks the number of connection and their "
        "utilization\n- vacuum - checks vacuum health for transaction id "
        "wraparound\n- sequence - checks sequences at risk of exceeding their "
        "maximum value\n- replication - checks replication health including "
        "lag and slots\n- buffer - checks for buffer cache hit rates for "
        "indexes and tables\n- constraint - checks for invalid constraints\n- "
        "all - runs all checks\nYou can optionally specify a single health "
        "check or a comma-separated list of health checks. The default is "
        "'all' checks.",
    )
    server.add_tool(
        get_top_queries,
        description="Reports the slowest or most resource-intensive queries "
        f"using data from the '{PG_STAT_STATEMENTS}' extension.",
        name="get_top_queries",
    )
    server.add_tool(
        add_comment_to_object,
        description="Adds a comment to a database object.",
        name="add_comment_to_object",
    )

    # Note: execute_sql will be added separately in main() based on
    # access mode (UNRESTRICTED vs RESTRICTED)

    auth_status = (
        "with authentication protection"
        if auth_context is not None
        else "without authentication"
    )
    logger.info(f"Registered 9 tools {auth_status}")

    return server


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
    """
    Retrieve top queries from pg_stat_statements.

    Args:
        sort_by: Ranking criteria ('total_time', 'mean_time', or 'resources')
        limit: Number of queries to return for time-based sorting
    """
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


def _setup_signal_handlers(transport: str):
    """Configure OS signal handlers and set the global transport flag."""
    global is_stdio_transport
    is_stdio_transport = transport == "stdio"
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


async def _run_transport(settings: ServerSettings):
    """Run the MCP server with the configured transport."""
    if settings.transport == "stdio":
        await mcp.run_stdio_async()
    elif settings.transport == "sse":
        mcp.settings.host = settings.sse_host
        mcp.settings.port = settings.sse_port
        await mcp.run_sse_async()
    elif settings.transport == "streamable-http":
        mcp.settings.host = settings.streamable_http_host
        mcp.settings.port = settings.streamable_http_port
        await mcp.run_streamable_http_async()


async def main():
    # Load settings from environment variables first
    settings = ServerSettings()

    # Parse command line arguments (they override environment variables)
    parser = argparse.ArgumentParser(
        description="Pg Airman MCP Server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Environment Variables:
  All CLI arguments can be set via environment variables with AIRMAN_MCP_ prefix.
  For example: AIRMAN_MCP_DATABASE_URL, AIRMAN_MCP_ACCESS_MODE, etc.
  CLI arguments take precedence over environment variables.
        """,
    )
    parser.add_argument(
        "database_url",
        help="Database connection URL",
        nargs="?",
        default=settings.database_url,
    )
    parser.add_argument(
        "--access-mode",
        type=str,
        choices=[mode.value for mode in AccessMode],
        default=settings.access_mode,
        help=f"SQL access mode (default: {settings.access_mode})",
    )
    parser.add_argument(
        "--transport",
        type=str,
        choices=["stdio", "sse", "streamable-http"],
        default=settings.transport,
        help=f"MCP transport (default: {settings.transport})",
    )
    parser.add_argument(
        "--sse-host",
        type=str,
        default=settings.sse_host,
        help=f"SSE server host (default: {settings.sse_host})",
    )
    parser.add_argument(
        "--sse-port",
        type=int,
        default=settings.sse_port,
        help=f"SSE server port (default: {settings.sse_port})",
    )
    parser.add_argument(
        "--streamable-http-host",
        type=str,
        default=settings.streamable_http_host,
        help=f"Streamable HTTP host (default: {settings.streamable_http_host})",
    )
    parser.add_argument(
        "--streamable-http-port",
        type=int,
        default=settings.streamable_http_port,
        help=f"Streamable HTTP port (default: {settings.streamable_http_port})",
    )
    parser.add_argument(
        "--dns-rebinding-protection",
        action=argparse.BooleanOptionalAction,
        default=settings.dns_rebinding_protection,
        help="Enable DNS rebinding protection",
    )
    parser.add_argument(
        "--allowed-hosts",
        type=str,
        default=settings.allowed_hosts,
        help="Comma-separated allowed Host header values",
    )
    parser.add_argument(
        "--allowed-origins",
        type=str,
        default=settings.allowed_origins,
        help="Comma-separated allowed Origin header values",
    )
    # Authentication arguments (delegated to auth_config)
    add_auth_cli_args(parser, settings)

    args = parser.parse_args()

    # Override settings with CLI arguments
    settings.database_url = args.database_url or settings.database_url
    settings.access_mode = args.access_mode
    settings.transport = args.transport
    settings.sse_host = args.sse_host
    settings.sse_port = args.sse_port
    settings.streamable_http_host = args.streamable_http_host
    settings.streamable_http_port = args.streamable_http_port
    settings.dns_rebinding_protection = args.dns_rebinding_protection
    settings.allowed_hosts = args.allowed_hosts
    settings.allowed_origins = args.allowed_origins

    # Auth CLI overrides (delegated to auth_config)
    apply_auth_cli_overrides(settings, args)

    # Parse required scopes
    required_scopes = settings.get_required_scopes()

    # Determine server URL based on transport
    server_url = settings.determine_server_url()
    if settings.auth_enabled and settings.transport == "stdio" and not server_url:
        raise ValueError(
            "When using --auth-enabled with stdio transport, you must set "
            "AIRMAN_MCP_SERVER_URL environment variable (e.g., http://localhost:8000)"
        )

    # Create auth context if auth is enabled
    auth_context = None
    if settings.auth_enabled:
        auth_context = create_auth_context(
            auth_server_url=settings.auth_server_url,
            server_url=server_url,  # type: ignore[arg-type]
            introspection_endpoint=settings.auth_introspection_endpoint,
            required_scopes=required_scopes,
            validate_resource=settings.auth_validate_resource,
            introspection_client_id=settings.auth_introspection_client_id,
            introspection_client_secret=settings.auth_introspection_client_secret,
        )

    # Build transport security settings
    transport_security = settings.build_transport_security()

    # Create MCP server with authentication and transport security configuration
    global mcp
    mcp = create_mcp_server(
        auth_context=auth_context,
        transport_security=transport_security,
    )

    # Store the access mode in the global variable
    global current_access_mode
    current_access_mode = AccessMode(settings.access_mode)

    # Add the query tool with a description appropriate to the access mode
    if current_access_mode == AccessMode.UNRESTRICTED:
        mcp.add_tool(execute_sql, description="Execute any SQL query")
    else:
        mcp.add_tool(execute_sql, description="Execute a read-only SQL query")

    logger.info(f"Starting Pg Airman MCP Server in {current_access_mode.upper()} mode")

    # Get database URL
    database_url = settings.database_url

    if not database_url:
        raise ValueError(
            "Error: No database URL provided. Please specify via "
            "AIRMAN_MCP_DATABASE_URL environment variable or command-line argument."
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
    _setup_signal_handlers(settings.transport)
    try:
        logger.info("Server starting...")
        # Shutdown loop: Keeps server running until shutdown_event is set
        # via signal handler (SIGINT/SIGTERM). The loop is necessary because:
        # 1. stdio transport: run_stdio_async() blocks indefinitely on stdin
        #    and returns when stdin closes. Without the loop, server exits
        #    immediately on stdin closure instead of waiting for signal.
        # 2. SSE/HTTP transports: run_*_async() methods handle their own
        #    event loops but may return on certain conditions. The while loop
        #    ensures the server stays alive until explicitly signaled.
        # The asyncio.sleep(0.1) prevents a tight loop if transport methods
        # return immediately, while still checking shutdown_event frequently.
        while not shutdown_event.is_set():
            await _run_transport(settings)
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

    # Close token verifier HTTP client
    if _auth_context is not None:
        try:
            await _auth_context.token_verifier.close()
        except Exception as e:
            logger.error(f"Error closing token verifier: {e}")

    # Close database connections
    try:
        logger.info("Closing database connection...")
        await db_connection.close()
        logger.info("Closed database connections")
    except Exception as e:
        logger.error(f"Error closing database connections: {e}")

    # Exit with appropriate status code
    logger.info("Shutdown complete")
