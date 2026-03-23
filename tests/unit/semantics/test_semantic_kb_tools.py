"""Tests for semantic_kb_tools module."""

from unittest.mock import AsyncMock

import pytest

from pg_airman_mcp.semantics.semantic_kb_tools import (
    get_semantic_kb_stats,
    list_semantic_kbs,
    refresh_semantic_kb,
    search_semantic_kb,
)
from pg_airman_mcp.sql import SafeSqlDriver


class MockRow:
    def __init__(self, data):
        self.cells = data


class MockSqlDriver:
    def __init__(self):
        self.execute_query = AsyncMock(return_value=[])


@pytest.fixture
def mock_sql_driver():
    return MockSqlDriver()


@pytest.mark.asyncio
async def test_search_semantic_kb_basic(mock_sql_driver):
    """Test basic semantic KB search."""
    mock_sql_driver.execute_query.return_value = [
        MockRow(
            {
                "schema_name": "sales",
                "relation_name": "customers",
                "column_name": None,
                "entity_type": "Table",
                "definition": "Table: sales.customers",
                "comment": "Customer master data",
                "similarity": 0.92,
            }
        ),
        MockRow(
            {
                "schema_name": "sales",
                "relation_name": "customers",
                "column_name": "email",
                "entity_type": "Column",
                "definition": "Column: sales.customers.email",
                "comment": "Customer email address",
                "similarity": 0.85,
            }
        ),
    ]

    results = await search_semantic_kb(
        mock_sql_driver, "test_kb", "customer email", min_similarity=0.7
    )

    assert len(results) == 2
    assert results[0]["relation_name"] == "customers"
    assert results[0]["similarity"] == 0.92
    mock_sql_driver.execute_query.assert_called_once()


@pytest.mark.asyncio
async def test_search_semantic_kb_with_entity_filter(mock_sql_driver):
    """Test semantic KB search with entity type filter."""
    mock_sql_driver.execute_query.return_value = [
        MockRow(
            {
                "schema_name": "sales",
                "relation_name": "customers",
                "entity_type": "Table",
                "definition": "Table: sales.customers",
                "comment": "Customer master data",
                "similarity": 0.92,
            }
        ),
    ]

    results = await search_semantic_kb(
        mock_sql_driver,
        "test_kb",
        "customer",
        entity_types=["Table", "View"],
    )

    assert len(results) == 1
    assert results[0]["entity_type"] == "Table"
    # Check that the entity array was built into the query
    call_args = mock_sql_driver.execute_query.call_args
    assert "get_entity_definitions" in call_args[0][0]
    # Verify column_name is included for consistent return shape
    assert "column_name" in call_args[0][0]


@pytest.mark.asyncio
async def test_search_semantic_kb_no_results(mock_sql_driver):
    """Test semantic KB search with no results."""
    mock_sql_driver.execute_query.return_value = []

    results = await search_semantic_kb(
        mock_sql_driver, "test_kb", "nonexistent", min_similarity=0.9
    )

    assert results == []


@pytest.mark.asyncio
async def test_list_semantic_kbs(mock_sql_driver):
    """Test listing semantic knowledge bases."""
    mock_sql_driver.execute_query.return_value = [
        MockRow(
            {
                "name": "analytics_kb",
                "model_name": "text-embedding-ada-002",
                "schemas": ["public", "sales"],
                "auto_processing": "Live",
            }
        ),
        MockRow(
            {
                "name": "hr_kb",
                "model_name": "text-embedding-ada-002",
                "schemas": ["hr"],
                "auto_processing": "Background",
            }
        ),
    ]

    results = await list_semantic_kbs(mock_sql_driver)

    assert len(results) == 2
    assert results[0]["name"] == "analytics_kb"
    assert results[1]["name"] == "hr_kb"


@pytest.mark.asyncio
async def test_list_semantic_kbs_empty(mock_sql_driver):
    """Test listing when no KBs exist."""
    mock_sql_driver.execute_query.return_value = []

    results = await list_semantic_kbs(mock_sql_driver)

    assert results == []


@pytest.mark.asyncio
async def test_get_semantic_kb_stats(mock_sql_driver):
    """Test getting semantic KB statistics."""
    mock_sql_driver.execute_query.return_value = [
        MockRow(
            {
                "total": 150,
                "tables": 10,
                "views": 5,
                "columns": 135,
                "pending": 0,
            }
        ),
    ]

    result = await get_semantic_kb_stats(mock_sql_driver, "test_kb")

    assert result["total"] == 150
    assert result["tables"] == 10
    assert result["columns"] == 135
    assert result["pending"] == 0


@pytest.mark.asyncio
async def test_get_semantic_kb_stats_not_found(mock_sql_driver):
    """Test getting stats for nonexistent KB."""
    mock_sql_driver.execute_query.return_value = []

    result = await get_semantic_kb_stats(mock_sql_driver, "nonexistent_kb")

    assert result == {}


@pytest.mark.asyncio
async def test_refresh_semantic_kb(mock_sql_driver):
    """Test refreshing semantic KB."""
    mock_sql_driver.execute_query.return_value = None

    result = await refresh_semantic_kb(mock_sql_driver, "test_kb")

    assert "Refreshed" in result
    assert "test_kb" in result
    mock_sql_driver.execute_query.assert_called_once()


@pytest.mark.asyncio
async def test_search_semantic_kb_with_column_entity_type(mock_sql_driver):
    """Test Column in entity_types falls back to get_metadata."""
    mock_sql_driver.execute_query.return_value = [
        MockRow(
            {
                "schema_name": "sales",
                "relation_name": "customers",
                "column_name": "email",
                "entity_type": "Column",
                "definition": "Column: sales.customers.email",
                "comment": "Customer email address",
                "similarity": 0.90,
            }
        ),
        MockRow(
            {
                "schema_name": "sales",
                "relation_name": "customers",
                "column_name": None,
                "entity_type": "Table",
                "definition": "Table: sales.customers",
                "comment": "Customer master data",
                "similarity": 0.85,
            }
        ),
        MockRow(
            {
                "schema_name": "sales",
                "relation_name": "orders",
                "column_name": None,
                "entity_type": "View",
                "definition": "View: sales.orders",
                "comment": "Order summary view",
                "similarity": 0.80,
            }
        ),
    ]

    results = await search_semantic_kb(
        mock_sql_driver,
        "test_kb",
        "customer",
        entity_types=["Column", "Table"],
    )

    # View row should be filtered out
    assert len(results) == 2
    assert results[0]["entity_type"] == "Column"
    assert results[0]["column_name"] == "email"
    assert results[1]["entity_type"] == "Table"
    # Should use get_metadata (not get_entity_definitions) for real column_name
    call_args = mock_sql_driver.execute_query.call_args
    assert "get_metadata" in call_args[0][0]


@pytest.mark.asyncio
async def test_search_semantic_kb_column_filter_respects_limit(mock_sql_driver):
    """Test that Column fallback path respects the limit after filtering."""
    mock_sql_driver.execute_query.return_value = [
        MockRow(
            {
                "schema_name": "s",
                "relation_name": "t",
                "column_name": f"col{i}",
                "entity_type": "Column",
                "definition": f"Column: s.t.col{i}",
                "comment": f"Column {i}",
                "similarity": 0.9 - i * 0.01,
            }
        )
        for i in range(5)
    ]

    results = await search_semantic_kb(
        mock_sql_driver,
        "test_kb",
        "columns",
        entity_types=["Column"],
        limit=3,
    )

    # Should trim to limit after filtering
    assert len(results) == 3
    assert results[0]["column_name"] == "col0"
    assert results[2]["column_name"] == "col2"


@pytest.mark.asyncio
async def test_search_semantic_kb_invalid_entity_type(mock_sql_driver):
    """Test that invalid entity types are rejected to prevent SQL injection."""
    with pytest.raises(ValueError, match="Invalid entity types"):
        await search_semantic_kb(
            mock_sql_driver,
            "test_kb",
            "customer",
            entity_types=["Table", "InvalidType'; DROP TABLE users; --"],
        )

    # Verify no query was executed
    mock_sql_driver.execute_query.assert_not_called()


@pytest.mark.asyncio
async def test_search_semantic_kb_invalid_min_similarity(mock_sql_driver):
    """Test that min_similarity outside 0.0-1.0 is rejected."""
    with pytest.raises(ValueError, match="min_similarity must be between"):
        await search_semantic_kb(
            mock_sql_driver, "test_kb", "customer", min_similarity=-0.5
        )

    with pytest.raises(ValueError, match="min_similarity must be between"):
        await search_semantic_kb(
            mock_sql_driver, "test_kb", "customer", min_similarity=1.5
        )

    mock_sql_driver.execute_query.assert_not_called()


@pytest.mark.asyncio
async def test_search_semantic_kb_invalid_limit(mock_sql_driver):
    """Test that limit outside valid range is rejected."""
    with pytest.raises(ValueError, match="limit must be at least 1"):
        await search_semantic_kb(mock_sql_driver, "test_kb", "customer", limit=0)

    with pytest.raises(ValueError, match="limit must be at least 1"):
        await search_semantic_kb(mock_sql_driver, "test_kb", "customer", limit=-5)

    with pytest.raises(ValueError, match="limit must not exceed 1000"):
        await search_semantic_kb(mock_sql_driver, "test_kb", "customer", limit=1001)

    mock_sql_driver.execute_query.assert_not_called()


# ============================================
# Bug reproduction: SafeSqlDriver rejects %s placeholders
# These tests verify that semantic KB tools work through
# SafeSqlDriver (restricted mode) without pglast choking
# on parameterized query placeholders.
# ============================================


@pytest.fixture
def safe_sql_driver():
    """Create a SafeSqlDriver wrapping a mock SqlDriver."""
    mock_base = MockSqlDriver()
    driver = SafeSqlDriver(sql_driver=mock_base, timeout=None)  # type: ignore[arg-type]
    return driver, mock_base


@pytest.mark.asyncio
async def test_get_kb_stats_through_safe_driver(safe_sql_driver):
    """Bug: get_semantic_kb_stats fails in restricted mode.

    SafeSqlDriver._validate() uses pglast which can't parse %s
    placeholders, causing all parameterized semantic KB queries
    to fail with 'syntax error at or near %'.
    """
    driver, mock_base = safe_sql_driver
    mock_base.execute_query.return_value = [
        MockRow(
            {
                "total": 32,
                "tables": 6,
                "views": 0,
                "columns": 26,
                "pending": 0,
            }
        ),
    ]

    result = await get_semantic_kb_stats(driver, "my_schema_kb")

    assert result["total"] == 32
    assert result["tables"] == 6


@pytest.mark.asyncio
async def test_search_kb_no_filter_through_safe_driver(safe_sql_driver):
    """Bug: search_semantic_kb fails in restricted mode (no filter).

    The get_metadata query uses %s placeholders that pglast rejects.
    """
    driver, mock_base = safe_sql_driver
    mock_base.execute_query.return_value = [
        MockRow(
            {
                "schema_name": "public",
                "relation_name": "products",
                "column_name": None,
                "entity_type": "Table",
                "definition": "public.products table",
                "comment": None,
                "similarity": 0.85,
            }
        ),
    ]

    results = await search_semantic_kb(
        driver, "my_schema_kb", "products", min_similarity=0.0, limit=5
    )

    assert len(results) == 1
    assert results[0]["relation_name"] == "products"


@pytest.mark.asyncio
async def test_search_kb_table_filter_through_safe_driver(safe_sql_driver):
    """Bug: search_semantic_kb fails in restricted mode (Table filter).

    The get_entity_definitions query uses %s placeholders that
    pglast rejects.
    """
    driver, mock_base = safe_sql_driver
    mock_base.execute_query.return_value = [
        MockRow(
            {
                "schema_name": "public",
                "relation_name": "products",
                "column_name": None,
                "entity_type": "Table",
                "definition": "public.products table",
                "comment": None,
                "similarity": 0.85,
            }
        ),
    ]

    results = await search_semantic_kb(
        driver,
        "my_schema_kb",
        "products",
        min_similarity=0.0,
        entity_types=["Table"],
        limit=5,
    )

    assert len(results) == 1
    assert results[0]["entity_type"] == "Table"
