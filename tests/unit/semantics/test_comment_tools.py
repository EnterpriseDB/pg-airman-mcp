"""Tests for comment_tools module."""

from unittest.mock import AsyncMock, patch

import pytest

from pg_airman_mcp.semantics.comment_tools import remove_object_comment


class MockRow:
    def __init__(self, data):
        self.cells = data


class MockSqlDriver:
    def __init__(self):
        self.execute_query = AsyncMock(return_value=None)


@pytest.fixture
def mock_sql_driver():
    return MockSqlDriver()


@pytest.mark.asyncio
async def test_remove_object_comment_table(mock_sql_driver):
    """Test removing comment from a table."""
    with patch(
        "pg_airman_mcp.semantics.comment_tools.execute_comment_on",
        new_callable=AsyncMock,
    ) as mock_execute:
        result = await remove_object_comment(
            mock_sql_driver, "public", "table", "users"
        )

        mock_execute.assert_called_once_with(
            mock_sql_driver, "TABLE", ["public", "users"], None
        )
        assert "Removed comment" in result
        assert "table" in result
        assert "public.users" in result


@pytest.mark.asyncio
async def test_remove_object_comment_view(mock_sql_driver):
    """Test removing comment from a view."""
    with patch(
        "pg_airman_mcp.semantics.comment_tools.execute_comment_on",
        new_callable=AsyncMock,
    ) as mock_execute:
        result = await remove_object_comment(
            mock_sql_driver, "sales", "view", "customer_summary"
        )

        mock_execute.assert_called_once_with(
            mock_sql_driver, "VIEW", ["sales", "customer_summary"], None
        )
        assert "Removed comment" in result
        assert "view" in result


@pytest.mark.asyncio
async def test_remove_object_comment_column(mock_sql_driver):
    """Test removing comment from a column."""
    with patch(
        "pg_airman_mcp.semantics.comment_tools.execute_comment_on",
        new_callable=AsyncMock,
    ) as mock_execute:
        result = await remove_object_comment(
            mock_sql_driver, "public", "column", "users", column_name="email"
        )

        mock_execute.assert_called_once_with(
            mock_sql_driver, "COLUMN", ["public", "users", "email"], None
        )
        assert "Removed comment" in result
        assert "column" in result
        assert "public.users.email" in result


@pytest.mark.asyncio
async def test_remove_object_comment_invalid_type(mock_sql_driver):
    """Test that invalid object type raises ValueError."""
    with pytest.raises(ValueError) as exc_info:
        await remove_object_comment(mock_sql_driver, "public", "invalid_type", "test")

    assert "Unsupported object type" in str(exc_info.value)


@pytest.mark.asyncio
async def test_remove_object_comment_column_missing_name(mock_sql_driver):
    """Test that column type without column_name raises ValueError."""
    with pytest.raises(ValueError) as exc_info:
        await remove_object_comment(mock_sql_driver, "public", "column", "users")

    assert "column_name is required" in str(exc_info.value)
