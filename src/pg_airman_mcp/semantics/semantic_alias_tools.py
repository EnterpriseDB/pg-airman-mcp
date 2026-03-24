"""Semantic alias tools for business SQL management and discovery."""

import json
import logging
from typing import Any

from pg_airman_mcp.sql import SafeSqlDriver, SqlDriver
from pg_airman_mcp.sql.safe_sql import ALLOW_ALIAS_WRITE_IN_RESTRICTED

logger = logging.getLogger(__name__)


def _get_write_driver(
    sql_driver: SqlDriver | SafeSqlDriver,
) -> SqlDriver | SafeSqlDriver:
    """Get the appropriate driver for alias write operations.

    In restricted mode, if ALLOW_ALIAS_WRITE_IN_RESTRICTED is enabled,
    bypasses SafeSqlDriver and uses the underlying SqlDriver directly
    (same pattern as execute_comment_on).
    """
    if isinstance(sql_driver, SafeSqlDriver):
        if not ALLOW_ALIAS_WRITE_IN_RESTRICTED:
            raise ValueError(
                "Semantic alias writes are not permitted in restricted mode"
            )
        logger.info(
            "Alias write in restricted mode (ALLOW_ALIAS_WRITE_IN_RESTRICTED=true)"
        )
        return sql_driver.sql_driver
    return sql_driver


async def _resolve_model_name(
    sql_driver: SqlDriver | SafeSqlDriver,
    kb_name: str,
) -> str:
    """Look up the embedding model name from a semantic KB."""
    sql = """
        SELECT model_name
        FROM aidb.list_semantic_kbs()
        WHERE name = %s
    """
    rows = await sql_driver.execute_query(sql, [kb_name])
    if not rows:
        raise ValueError(f"Semantic KB '{kb_name}' not found")
    return rows[0].cells["model_name"]


async def create_semantic_alias(
    sql_driver: SqlDriver | SafeSqlDriver,
    kb_name: str,
    name: str,
    description: str,
    query_text: str,
    parameters: list[dict[str, Any]] | None = None,
) -> str:
    """Create a semantic alias for a reusable parameterized SQL query.

    Resolves the embedding model from the given semantic KB so that the
    alias description is vectorized and discoverable via search.

    Args:
        sql_driver: SqlDriver or SafeSqlDriver instance.
        kb_name: Name of the semantic knowledge base (used to resolve model).
        name: Unique name for the semantic alias.
        description: Business description (vectorized for search).
        query_text: Complete SQL query with ${param_name} placeholders.
        parameters: Optional JSONB array of {name, param_type, description}.

    Returns:
        Success message.
    """
    driver = _get_write_driver(sql_driver)

    model_name = await _resolve_model_name(sql_driver, kb_name)

    if parameters:
        params_json = json.dumps(parameters)
        sql = (
            "SELECT aidb.create_semantic_alias("
            "%s::text, %s::text, %s::text, %s::jsonb, %s::text)"
        )
        sql_params: list[Any] = [
            name,
            description,
            query_text,
            params_json,
            model_name,
        ]
    else:
        sql = (
            "SELECT aidb.create_semantic_alias("
            "%s::text, %s::text, %s::text, NULL::jsonb, %s::text)"
        )
        sql_params = [name, description, query_text, model_name]

    await driver.execute_query(sql, sql_params)

    logger.info(f"Created semantic alias '{name}'")
    return f"Created semantic alias '{name}'"


async def delete_semantic_alias(
    sql_driver: SqlDriver | SafeSqlDriver,
    name: str,
) -> str:
    """Delete a semantic alias.

    Args:
        sql_driver: SqlDriver or SafeSqlDriver instance.
        name: Name of the semantic alias to delete.

    Returns:
        Success message.
    """
    sql = "SELECT aidb.delete_semantic_alias(%s::text)"
    driver = _get_write_driver(sql_driver)
    await driver.execute_query(sql, [name])

    logger.info(f"Deleted semantic alias '{name}'")
    return f"Deleted semantic alias '{name}'"


async def search_semantic_aliases(
    sql_driver: SqlDriver | SafeSqlDriver,
    kb_name: str,
    query: str,
    min_similarity: float = 0.7,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Search for semantic aliases by natural language query.

    Resolves the embedding model from the given semantic KB, then calls
    aidb.search_semantic_aliases to perform vector-similarity search.

    Args:
        sql_driver: SqlDriver or SafeSqlDriver instance.
        kb_name: Name of the semantic knowledge base.
        query: Natural language description of what you're looking for.
        min_similarity: Minimum similarity threshold (0.0-1.0).
        limit: Maximum results to return (1-1000).

    Returns:
        List of matching aliases with name, query_text, and similarity.
    """
    if not 0.0 <= min_similarity <= 1.0:
        raise ValueError(
            f"min_similarity must be between 0.0 and 1.0, got {min_similarity}"
        )
    if limit < 1:
        raise ValueError(f"limit must be at least 1, got {limit}")
    if limit > 1000:
        raise ValueError(f"limit must not exceed 1000, got {limit}")

    model_name = await _resolve_model_name(sql_driver, kb_name)

    sql = """
        SELECT name, query_text, similarity
        FROM aidb.search_semantic_aliases(
            %s::text, %s::text, %s::double precision, %s::integer, 0
        )
        ORDER BY similarity DESC
    """
    rows = await sql_driver.execute_query(
        sql, [model_name, query, min_similarity, limit]
    )

    results = [row.cells for row in (rows or [])]
    logger.info(
        f"Semantic alias search in KB '{kb_name}' for '{query}' "
        f"returned {len(results)} results"
    )
    return results


async def execute_semantic_alias(
    sql_driver: SqlDriver | SafeSqlDriver,
    name: str,
    parameters: dict[str, Any] | None = None,
    execute_as_role: str | None = None,
) -> list[dict[str, Any]]:
    """Execute a semantic alias with provided parameters.

    AIDB signature:
        aidb.execute_semantic_alias(
            alias_name TEXT, args JSONB DEFAULT NULL,
            execute_role TEXT DEFAULT NULL
        )

    Args:
        sql_driver: SqlDriver or SafeSqlDriver instance.
        name: Name of the semantic alias to execute.
        parameters: Parameter values as {param_name: value}.
        execute_as_role: Optional role to execute as.

    Returns:
        Query result rows.
    """
    args_json = json.dumps(parameters) if parameters else None

    if execute_as_role:
        sql = "SELECT * FROM aidb.execute_semantic_alias(%s::text, %s::jsonb, %s::text)"
        sql_params: list[Any] = [name, args_json, execute_as_role]
    elif args_json:
        sql = "SELECT * FROM aidb.execute_semantic_alias(%s::text, %s::jsonb)"
        sql_params = [name, args_json]
    else:
        sql = "SELECT * FROM aidb.execute_semantic_alias(%s::text)"
        sql_params = [name]

    rows = await sql_driver.execute_query(sql, sql_params)

    # AIDB returns TABLE(result JSONB) where each row is a JSON object.
    # Unwrap the "result" wrapper to return flat dicts.
    results = [row.cells.get("result", row.cells) for row in (rows or [])]
    logger.info(f"Executed semantic alias '{name}', returned {len(results)} rows")
    return results


async def list_semantic_aliases(
    sql_driver: SqlDriver | SafeSqlDriver,
) -> list[dict[str, Any]]:
    """List all available semantic aliases.

    Args:
        sql_driver: SqlDriver or SafeSqlDriver instance.

    Returns:
        List of aliases with name, description, query_text, and param_count.
    """
    sql = "SELECT * FROM aidb.get_semantic_aliases()"
    rows = await sql_driver.execute_query(sql)

    results = [row.cells for row in (rows or [])]
    logger.info(f"Found {len(results)} semantic aliases")
    return results
