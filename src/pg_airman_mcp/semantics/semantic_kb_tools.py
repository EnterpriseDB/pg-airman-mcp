"""Semantic Knowledge Base retrieval tools."""

import logging
from typing import Any

from pg_airman_mcp.sql import SafeSqlDriver, SqlDriver

logger = logging.getLogger(__name__)

# Valid entity types for semantic KB search
VALID_ENTITY_TYPES = frozenset({"Table", "View", "Column"})


async def search_semantic_kb(
    sql_driver: SqlDriver | SafeSqlDriver,
    kb_name: str,
    query: str,
    min_similarity: float = 0.7,
    entity_types: list[str] | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Search the semantic knowledge base for relevant tables/columns.

    Args:
        sql_driver: SqlDriver or SafeSqlDriver instance.
        kb_name: Name of the semantic knowledge base to search.
        query: Natural language query describing what you're looking for.
        min_similarity: Minimum similarity threshold (0.0-1.0).
        entity_types: Optional filter by entity types: 'Table', 'View', 'Column'.
        limit: Maximum number of results to return (1-1000).

    Returns:
        List of matching entities with schema, relation, column info and similarity.
    """
    # Validate min_similarity
    if not 0.0 <= min_similarity <= 1.0:
        raise ValueError(
            f"min_similarity must be between 0.0 and 1.0, got {min_similarity}"
        )

    # Validate limit
    if limit < 1:
        raise ValueError(f"limit must be at least 1, got {limit}")
    if limit > 1000:
        raise ValueError(f"limit must not exceed 1000, got {limit}")

    if entity_types:
        # Validate entity types to prevent SQL injection
        invalid_types = set(entity_types) - VALID_ENTITY_TYPES
        if invalid_types:
            raise ValueError(
                f"Invalid entity types: {invalid_types}. "
                f"Allowed: {sorted(VALID_ENTITY_TYPES)}"
            )

        entity_type_set = set(entity_types)

        if "Column" in entity_type_set:
            # Fall back to get_metadata which returns real column_name,
            # then filter by entity_type in Python.
            # Request extra rows to compensate for post-filter reduction.
            fetch_limit = min(limit * 3, 1000)
            sql = """
                SELECT schema_name, relation_name, column_name,
                       entity_type, definition, comment, similarity
                FROM aidb.get_metadata(%s, %s, %s, %s, 0)
                ORDER BY similarity DESC
            """
            rows = await sql_driver.execute_query(
                sql, [kb_name, query, min_similarity, fetch_limit]
            )
            all_results = [row.cells for row in (rows or [])]
            results = [
                r for r in all_results if r.get("entity_type") in entity_type_set
            ][:limit]
        else:
            # Table/View only — use get_entity_definitions (no column_name)
            # Safe to interpolate after validation against allowlist
            entity_array = (
                "ARRAY[" + ",".join(f"'{t}'" for t in entity_types) + "]::text[]"
            )
            sql = f"""
                SELECT schema_name, relation_name,
                       NULL as column_name, entity_type,
                       definition, comment, similarity
                FROM aidb.get_entity_definitions(
                    %s, %s, %s, {entity_array}, %s, 0
                )
                ORDER BY similarity DESC
            """
            rows = await sql_driver.execute_query(
                sql,  # type: ignore[arg-type]
                [kb_name, query, min_similarity, limit],
            )
            results = [row.cells for row in (rows or [])]
    else:
        # Use aidb.get_metadata for full search
        sql = """
            SELECT schema_name, relation_name, column_name, entity_type,
                   definition, comment, similarity
            FROM aidb.get_metadata(%s, %s, %s, %s, 0)
            ORDER BY similarity DESC
        """
        rows = await sql_driver.execute_query(
            sql, [kb_name, query, min_similarity, limit]
        )
        results = [row.cells for row in (rows or [])]
    logger.info(
        f"Semantic KB search in '{kb_name}' for '{query}' "
        f"returned {len(results)} results"
    )
    return results


async def list_semantic_kbs(
    sql_driver: SqlDriver | SafeSqlDriver,
) -> list[dict[str, Any]]:
    """List all available semantic knowledge bases.

    Args:
        sql_driver: SqlDriver or SafeSqlDriver instance.

    Returns:
        List of KBs with name, model_name, schemas, and auto_processing info.
    """
    sql = """
        SELECT name, model_name, schemas, auto_processing
        FROM aidb.list_semantic_kbs()
    """
    rows = await sql_driver.execute_query(sql)

    results = [row.cells for row in (rows or [])]
    logger.info(f"Found {len(results)} semantic knowledge bases")
    return results


async def get_semantic_kb_stats(
    sql_driver: SqlDriver | SafeSqlDriver,
    kb_name: str,
) -> dict[str, Any]:
    """Get statistics about a semantic knowledge base.

    Args:
        sql_driver: SqlDriver or SafeSqlDriver instance.
        kb_name: Name of the semantic knowledge base.

    Returns:
        Dict with total, tables, views, columns, and pending counts.
    """
    sql = """
        SELECT total, tables, views, columns, pending
        FROM aidb.semantic_kb_stats(%s)
    """
    rows = await sql_driver.execute_query(sql, [kb_name])

    if rows:
        result = rows[0].cells
        logger.info(
            f"Semantic KB '{kb_name}' stats: {result.get('total', 0)} entities, "
            f"{result.get('pending', 0)} pending"
        )
        return result
    return {}


async def refresh_semantic_kb(
    sql_driver: SqlDriver | SafeSqlDriver,
    kb_name: str,
) -> str:
    """Refresh the semantic knowledge base to sync with current schema metadata.

    Args:
        sql_driver: SqlDriver or SafeSqlDriver instance.
        kb_name: Name of the semantic knowledge base to refresh.

    Returns:
        Success message.
    """
    sql = "SELECT aidb.refresh_semantic_kb(%s)"
    await sql_driver.execute_query(sql, [kb_name])

    logger.info(f"Refreshed semantic KB '{kb_name}'")
    return f"Refreshed semantic knowledge base '{kb_name}'"
