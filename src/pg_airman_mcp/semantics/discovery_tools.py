"""Combined discovery tool for SQL generation support."""

import logging
from typing import Any

from pg_airman_mcp.sql import SafeSqlDriver, SqlDriver

from .semantic_alias_tools import search_semantic_aliases
from .semantic_kb_tools import search_semantic_kb

logger = logging.getLogger(__name__)


async def discover_schema_for_question(
    sql_driver: SqlDriver | SafeSqlDriver,
    kb_name: str,
    question: str,
    include_aliases: bool = True,
    min_similarity: float = 0.6,
    max_results: int = 15,
) -> dict[str, Any]:
    """Combine semantic KB search and alias discovery for SQL generation.

    Implements the "Semantic KB First" pattern in a single invocation,
    returning all context needed for text-to-SQL generation.

    Args:
        sql_driver: SqlDriver or SafeSqlDriver instance.
        kb_name: Semantic knowledge base to search.
        question: The business question to find schema elements for.
        include_aliases: Whether to also search semantic aliases.
        min_similarity: Minimum similarity threshold (0.0-1.0).
        max_results: Maximum results per category.

    Returns:
        Dict with schema_metadata and existing_aliases lists.
    """
    results: dict[str, Any] = {
        "schema_metadata": [],
        "existing_aliases": [],
    }

    metadata = await search_semantic_kb(
        sql_driver,
        kb_name=kb_name,
        query=question,
        min_similarity=min_similarity,
        limit=max_results,
    )
    results["schema_metadata"] = metadata

    if include_aliases:
        aliases = await search_semantic_aliases(
            sql_driver,
            kb_name=kb_name,
            query=question,
            min_similarity=min_similarity,
            limit=max_results,
        )
        results["existing_aliases"] = aliases

    logger.debug(
        f"Discovery for '{question}': "
        f"{len(metadata)} schema matches, "
        f"{len(results['existing_aliases'])} alias matches"
    )
    return results
