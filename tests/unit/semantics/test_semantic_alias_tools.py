"""Tests for semantic_alias_tools module."""

from unittest.mock import AsyncMock, patch

import pytest

from pg_airman_mcp.semantics.semantic_alias_tools import (
    create_semantic_alias,
    delete_semantic_alias,
    execute_semantic_alias,
    list_semantic_aliases,
    search_semantic_aliases,
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


# ======================
# create_semantic_alias
# ======================


@pytest.mark.asyncio
async def test_create_semantic_alias_basic(mock_sql_driver):
    """Test creating a semantic alias without parameters."""
    mock_sql_driver.execute_query.side_effect = [
        # First call: _resolve_model_name
        [MockRow({"model_name": "my_model"})],
        # Second call: actual create
        None,
    ]

    result = await create_semantic_alias(
        mock_sql_driver,
        kb_name="my_kb",
        name="monthly_revenue",
        description="Calculate total revenue by month",
        query_text=(
            "SELECT date_trunc('month', order_date) AS month,"
            " SUM(total) FROM orders GROUP BY 1"
        ),
    )

    assert "Created" in result
    assert "monthly_revenue" in result
    # Second call is the CREATE
    call_args = mock_sql_driver.execute_query.call_args_list[1]
    sql = call_args[0][0]
    assert "aidb.create_semantic_alias" in sql
    # NULL params must be explicitly typed for correct function resolution
    assert "NULL::jsonb" in sql
    assert call_args[0][1][0] == "monthly_revenue"
    assert call_args[0][1][1] == "Calculate total revenue by month"
    # Model should be passed as last param
    assert call_args[0][1][-1] == "my_model"


@pytest.mark.asyncio
async def test_create_semantic_alias_with_parameters(mock_sql_driver):
    """Test creating a semantic alias with JSONB parameters."""
    mock_sql_driver.execute_query.side_effect = [
        [MockRow({"model_name": "my_model"})],
        None,
    ]

    params = [{"name": "email", "param_type": "text", "description": "Customer email"}]
    result = await create_semantic_alias(
        mock_sql_driver,
        kb_name="my_kb",
        name="customer_by_email",
        description="Find customer by their email address",
        query_text="SELECT * FROM customers WHERE email = ${email}",
        parameters=params,
    )

    assert "Created" in result
    call_args = mock_sql_driver.execute_query.call_args_list[1]
    sql = call_args[0][0]
    assert "aidb.create_semantic_alias" in sql
    assert "jsonb" in sql
    sql_params = call_args[0][1]
    assert sql_params[0] == "customer_by_email"
    # The JSONB params should be the 4th parameter
    assert '"email"' in sql_params[3]
    # Model should be the 5th parameter
    assert sql_params[4] == "my_model"


# ======================
# delete_semantic_alias
# ======================


@pytest.mark.asyncio
async def test_delete_semantic_alias(mock_sql_driver):
    """Test deleting a semantic alias."""
    mock_sql_driver.execute_query.return_value = None

    result = await delete_semantic_alias(mock_sql_driver, "monthly_revenue")

    assert "Deleted" in result
    assert "monthly_revenue" in result
    call_args = mock_sql_driver.execute_query.call_args
    assert "aidb.delete_semantic_alias" in call_args[0][0]
    assert call_args[0][1] == ["monthly_revenue"]


# =========================
# search_semantic_aliases
# =========================


@pytest.mark.asyncio
async def test_search_semantic_aliases_basic(mock_sql_driver):
    """Test searching for semantic aliases."""
    mock_sql_driver.execute_query.side_effect = [
        # First call: _resolve_model_name
        [MockRow({"model_name": "my_model"})],
        # Second call: actual search
        [
            MockRow(
                {
                    "name": "customer_by_email",
                    "query_text": ("SELECT * FROM customers WHERE email = ${email}"),
                    "similarity": 0.85,
                }
            ),
            MockRow(
                {
                    "name": "customer_ltv",
                    "query_text": (
                        "SELECT customer_id, SUM(total) FROM orders GROUP BY 1"
                    ),
                    "similarity": 0.72,
                }
            ),
        ],
    ]

    results = await search_semantic_aliases(
        mock_sql_driver, "my_kb", "find customer", min_similarity=0.7
    )

    assert len(results) == 2
    assert results[0]["name"] == "customer_by_email"
    assert results[0]["similarity"] == 0.85
    # First call resolves model name from KB
    resolve_args = mock_sql_driver.execute_query.call_args_list[0]
    assert "aidb.list_semantic_kbs" in resolve_args[0][0]
    assert resolve_args[0][1] == ["my_kb"]
    # Second call performs the search with resolved model
    search_args = mock_sql_driver.execute_query.call_args_list[1]
    assert "aidb.search_semantic_aliases" in search_args[0][0]
    assert search_args[0][1][0] == "my_model"


@pytest.mark.asyncio
async def test_search_semantic_aliases_no_results(mock_sql_driver):
    """Test search with no matching aliases."""
    mock_sql_driver.execute_query.side_effect = [
        [MockRow({"model_name": "my_model"})],
        [],
    ]

    results = await search_semantic_aliases(
        mock_sql_driver, "my_kb", "nonexistent query"
    )

    assert results == []


@pytest.mark.asyncio
async def test_search_semantic_aliases_kb_not_found(mock_sql_driver):
    """Test that a missing KB raises ValueError."""
    mock_sql_driver.execute_query.return_value = []

    with pytest.raises(ValueError, match="Semantic KB 'missing_kb' not found"):
        await search_semantic_aliases(mock_sql_driver, "missing_kb", "query")


@pytest.mark.asyncio
async def test_search_semantic_aliases_invalid_similarity(mock_sql_driver):
    """Test that invalid min_similarity is rejected."""
    with pytest.raises(ValueError, match="min_similarity must be between"):
        await search_semantic_aliases(
            mock_sql_driver, "kb", "query", min_similarity=-0.5
        )

    with pytest.raises(ValueError, match="min_similarity must be between"):
        await search_semantic_aliases(
            mock_sql_driver, "kb", "query", min_similarity=1.5
        )

    mock_sql_driver.execute_query.assert_not_called()


@pytest.mark.asyncio
async def test_search_semantic_aliases_invalid_limit(mock_sql_driver):
    """Test that invalid limit is rejected."""
    with pytest.raises(ValueError, match="limit must be at least 1"):
        await search_semantic_aliases(mock_sql_driver, "kb", "query", limit=0)

    with pytest.raises(ValueError, match="limit must not exceed 1000"):
        await search_semantic_aliases(mock_sql_driver, "kb", "query", limit=1001)

    mock_sql_driver.execute_query.assert_not_called()


# ==========================
# execute_semantic_alias
# ==========================


@pytest.mark.asyncio
async def test_execute_semantic_alias_no_params(mock_sql_driver):
    """Test executing a semantic alias without parameters."""
    mock_sql_driver.execute_query.return_value = [
        MockRow({"id": 1, "name": "Alice"}),
        MockRow({"id": 2, "name": "Bob"}),
    ]

    results = await execute_semantic_alias(mock_sql_driver, "all_customers")

    assert len(results) == 2
    assert results[0]["name"] == "Alice"
    call_args = mock_sql_driver.execute_query.call_args
    assert "aidb.execute_semantic_alias" in call_args[0][0]
    assert call_args[0][1] == ["all_customers"]


@pytest.mark.asyncio
async def test_execute_semantic_alias_with_params(mock_sql_driver):
    """Test executing a semantic alias with parameters."""
    mock_sql_driver.execute_query.return_value = [
        MockRow({"id": 1, "email": "alice@example.com", "name": "Alice"}),
    ]

    results = await execute_semantic_alias(
        mock_sql_driver,
        "customer_by_email",
        parameters={"email": "alice@example.com"},
    )

    assert len(results) == 1
    assert results[0]["email"] == "alice@example.com"
    call_args = mock_sql_driver.execute_query.call_args
    sql = call_args[0][0]
    assert "aidb.execute_semantic_alias" in sql
    assert "jsonb" in sql


@pytest.mark.asyncio
async def test_execute_semantic_alias_with_role(mock_sql_driver):
    """Test executing a semantic alias with a specific role."""
    mock_sql_driver.execute_query.return_value = []

    await execute_semantic_alias(
        mock_sql_driver,
        "admin_query",
        parameters={"id": 1},
        execute_as_role="readonly_user",
    )

    call_args = mock_sql_driver.execute_query.call_args
    assert call_args[0][1][2] == "readonly_user"


@pytest.mark.asyncio
async def test_execute_semantic_alias_empty_results(mock_sql_driver):
    """Test executing a semantic alias that returns no rows."""
    mock_sql_driver.execute_query.return_value = []

    results = await execute_semantic_alias(mock_sql_driver, "empty_query")

    assert results == []


# ==========================
# list_semantic_aliases
# ==========================


@pytest.mark.asyncio
async def test_list_semantic_aliases(mock_sql_driver):
    """Test listing all semantic aliases."""
    mock_sql_driver.execute_query.return_value = [
        MockRow(
            {
                "name": "customer_by_email",
                "description": "Find customer",
                "query_text": "SELECT 1",
                "param_count": 1,
            }
        ),
        MockRow(
            {
                "name": "monthly_revenue",
                "description": "Revenue",
                "query_text": "SELECT 2",
                "param_count": 0,
            }
        ),
    ]

    results = await list_semantic_aliases(mock_sql_driver)

    assert len(results) == 2
    assert results[0]["name"] == "customer_by_email"
    assert results[1]["name"] == "monthly_revenue"
    call_args = mock_sql_driver.execute_query.call_args
    assert "aidb.get_semantic_aliases" in call_args[0][0]


@pytest.mark.asyncio
async def test_list_semantic_aliases_empty(mock_sql_driver):
    """Test listing when no aliases exist."""
    mock_sql_driver.execute_query.return_value = []

    results = await list_semantic_aliases(mock_sql_driver)

    assert results == []


# ============================================
# SafeSqlDriver compatibility tests
# ============================================


@pytest.fixture
def safe_sql_driver():
    """Create a SafeSqlDriver wrapping a mock SqlDriver."""
    mock_base = MockSqlDriver()
    driver = SafeSqlDriver(sql_driver=mock_base, timeout=None)  # type: ignore[arg-type]
    return driver, mock_base


@pytest.mark.asyncio
async def test_search_aliases_through_safe_driver(safe_sql_driver):
    """Test that search_semantic_aliases works through SafeSqlDriver."""
    driver, mock_base = safe_sql_driver
    mock_base.execute_query.side_effect = [
        # First call: _resolve_model_name
        [MockRow({"model_name": "my_model"})],
        # Second call: actual search
        [
            MockRow(
                {
                    "name": "customer_by_email",
                    "query_text": ("SELECT * FROM customers WHERE email = ${email}"),
                    "similarity": 0.85,
                }
            ),
        ],
    ]

    results = await search_semantic_aliases(
        driver, "my_kb", "find customer", min_similarity=0.0
    )

    assert len(results) == 1
    assert results[0]["name"] == "customer_by_email"


@pytest.mark.asyncio
async def test_list_aliases_through_safe_driver(safe_sql_driver):
    """Test that list_semantic_aliases works through SafeSqlDriver."""
    driver, mock_base = safe_sql_driver
    mock_base.execute_query.return_value = [
        MockRow(
            {
                "name": "alias1",
                "description": "desc1",
                "query_text": "SELECT 1",
                "param_count": 0,
            }
        ),
        MockRow(
            {
                "name": "alias2",
                "description": "desc2",
                "query_text": "SELECT 2",
                "param_count": 1,
            }
        ),
    ]

    results = await list_semantic_aliases(driver)

    assert len(results) == 2
    assert results[0]["name"] == "alias1"


# ============================================
# ALLOW_ALIAS_WRITE_IN_RESTRICTED flag tests
# ============================================


@pytest.mark.asyncio
async def test_create_alias_blocked_in_restricted_by_default(
    safe_sql_driver,
):
    """Create alias via SafeSqlDriver is blocked when flag is false."""
    driver, mock_base = safe_sql_driver
    # _resolve_model_name reads through SafeSqlDriver (allowed)
    mock_base.execute_query.return_value = [MockRow({"model_name": "my_model"})]

    with patch(
        "pg_airman_mcp.semantics.semantic_alias_tools.ALLOW_ALIAS_WRITE_IN_RESTRICTED",
        False,
    ):
        with pytest.raises(ValueError, match="not permitted"):
            await create_semantic_alias(
                driver,
                kb_name="my_kb",
                name="test",
                description="desc",
                query_text="SELECT 1",
            )


@pytest.mark.asyncio
async def test_create_alias_allowed_in_restricted_with_flag(
    safe_sql_driver,
):
    """Create alias via SafeSqlDriver succeeds when flag is true."""
    driver, mock_base = safe_sql_driver
    mock_base.execute_query.side_effect = [
        # First call: _resolve_model_name
        [MockRow({"model_name": "my_model"})],
        # Second call: actual create (via underlying driver)
        None,
    ]

    with patch(
        "pg_airman_mcp.semantics.semantic_alias_tools.ALLOW_ALIAS_WRITE_IN_RESTRICTED",
        True,
    ):
        result = await create_semantic_alias(
            driver,
            kb_name="my_kb",
            name="test",
            description="desc",
            query_text="SELECT 1",
        )

    assert "Created" in result


@pytest.mark.asyncio
async def test_delete_alias_blocked_in_restricted_by_default(
    safe_sql_driver,
):
    """Delete alias via SafeSqlDriver is blocked when flag is false."""
    driver, _mock_base = safe_sql_driver

    with patch(
        "pg_airman_mcp.semantics.semantic_alias_tools.ALLOW_ALIAS_WRITE_IN_RESTRICTED",
        False,
    ):
        with pytest.raises(ValueError, match="not permitted"):
            await delete_semantic_alias(driver, "test")


@pytest.mark.asyncio
async def test_delete_alias_allowed_in_restricted_with_flag(
    safe_sql_driver,
):
    """Delete alias via SafeSqlDriver succeeds when flag is true."""
    driver, mock_base = safe_sql_driver
    mock_base.execute_query.return_value = None

    with patch(
        "pg_airman_mcp.semantics.semantic_alias_tools.ALLOW_ALIAS_WRITE_IN_RESTRICTED",
        True,
    ):
        result = await delete_semantic_alias(driver, "test")

    assert "Deleted" in result
    mock_base.execute_query.assert_called_once()
