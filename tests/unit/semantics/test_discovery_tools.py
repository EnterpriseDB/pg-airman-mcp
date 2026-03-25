"""Tests for discovery_tools module."""

from unittest.mock import AsyncMock

import pytest

from pg_airman_mcp.semantics.discovery_tools import discover_schema_for_question


class MockRow:
    def __init__(self, data):
        self.cells = data


class MockSqlDriver:
    def __init__(self):
        self.execute_query = AsyncMock(return_value=[])


@pytest.fixture
def mock_sql_driver():
    return MockSqlDriver()


# ======================
# discover_schema_for_question
# ======================


@pytest.mark.asyncio
async def test_discover_returns_both_metadata_and_aliases(mock_sql_driver):
    """Test that discovery returns both schema metadata and aliases."""
    mock_sql_driver.execute_query.side_effect = [
        # search_semantic_kb call
        [
            MockRow(
                {
                    "schema_name": "public",
                    "relation_name": "orders",
                    "column_name": None,
                    "entity_type": "Table",
                    "definition": "CREATE TABLE orders ...",
                    "comment": "Customer orders table",
                    "similarity": 0.85,
                }
            ),
        ],
        # _resolve_model_name for search_semantic_aliases
        [MockRow({"model_name": "my_model"})],
        # search_semantic_aliases call
        [
            MockRow(
                {
                    "name": "monthly_revenue",
                    "query_text": "SELECT SUM(total) FROM orders GROUP BY month",
                    "similarity": 0.78,
                }
            ),
        ],
    ]

    results = await discover_schema_for_question(
        mock_sql_driver,
        kb_name="my_kb",
        question="how much revenue per month",
    )

    assert len(results["schema_metadata"]) == 1
    assert results["schema_metadata"][0]["relation_name"] == "orders"
    assert len(results["existing_aliases"]) == 1
    assert results["existing_aliases"][0]["name"] == "monthly_revenue"


@pytest.mark.asyncio
async def test_discover_without_aliases(mock_sql_driver):
    """Test discovery with include_aliases=False skips alias search."""
    mock_sql_driver.execute_query.side_effect = [
        # search_semantic_kb call only
        [
            MockRow(
                {
                    "schema_name": "public",
                    "relation_name": "customers",
                    "column_name": None,
                    "entity_type": "Table",
                    "definition": "CREATE TABLE customers ...",
                    "comment": "Customer info",
                    "similarity": 0.9,
                }
            ),
        ],
    ]

    results = await discover_schema_for_question(
        mock_sql_driver,
        kb_name="my_kb",
        question="find customers",
        include_aliases=False,
    )

    assert len(results["schema_metadata"]) == 1
    assert results["existing_aliases"] == []
    # Only one call (no alias search)
    assert mock_sql_driver.execute_query.call_count == 1


@pytest.mark.asyncio
async def test_discover_empty_results(mock_sql_driver):
    """Test discovery when nothing matches."""
    mock_sql_driver.execute_query.side_effect = [
        # search_semantic_kb: no results
        [],
        # _resolve_model_name
        [MockRow({"model_name": "my_model"})],
        # search_semantic_aliases: no results
        [],
    ]

    results = await discover_schema_for_question(
        mock_sql_driver,
        kb_name="my_kb",
        question="quantum physics equations",
    )

    assert results["schema_metadata"] == []
    assert results["existing_aliases"] == []


@pytest.mark.asyncio
async def test_discover_passes_similarity_and_limit(mock_sql_driver):
    """Test that min_similarity and max_results are forwarded correctly."""
    mock_sql_driver.execute_query.side_effect = [
        [],  # search_semantic_kb
        [MockRow({"model_name": "my_model"})],  # _resolve_model_name
        [],  # search_semantic_aliases
    ]

    await discover_schema_for_question(
        mock_sql_driver,
        kb_name="my_kb",
        question="test",
        min_similarity=0.3,
        max_results=5,
    )

    # First call: search_semantic_kb
    kb_call = mock_sql_driver.execute_query.call_args_list[0]
    kb_params = kb_call[0][1]
    assert kb_params[2] == 0.3  # min_similarity
    assert kb_params[3] == 5  # limit

    # Third call: search_semantic_aliases
    alias_call = mock_sql_driver.execute_query.call_args_list[2]
    alias_params = alias_call[0][1]
    assert alias_params[2] == 0.3  # min_similarity
    assert alias_params[3] == 5  # limit


@pytest.mark.asyncio
async def test_discover_propagates_kb_error(mock_sql_driver):
    """Test that errors from search_semantic_kb propagate."""
    mock_sql_driver.execute_query.side_effect = Exception("DB connection failed")

    with pytest.raises(Exception, match="DB connection failed"):
        await discover_schema_for_question(
            mock_sql_driver,
            kb_name="my_kb",
            question="test",
        )
