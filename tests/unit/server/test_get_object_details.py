import ast

import mcp.types as types
import pytest

from pg_airman_mcp.server import get_object_details
from pg_airman_mcp.sql import SqlDriver


class MockRow:
    def __init__(self, data):
        self.cells = data


class MockSqlDriver(SqlDriver):
    def __init__(self):  # type: ignore[override]
        # Bypass normal initialization; provide minimal attributes
        self.conn = object()
        self.is_pool = False
        self.query_results = {}

    def set_query_result(self, query_key: str, result):
        """Set a mock result for a specific query pattern."""
        self.query_results[query_key] = result

    async def execute_query(self, query, params=None):  # type: ignore[override]
        # Return pre-configured results based on query pattern
        for key, result in self.query_results.items():
            if key in query:
                return result
        return []


@pytest.fixture
def mock_sql_driver():
    """Create a mock SQL driver for testing."""
    return MockSqlDriver()


@pytest.mark.asyncio
async def test_get_object_details_table_with_comment(mock_sql_driver, monkeypatch):
    """Test get_object_details for table with object-level comment."""

    # Mock the get_sql_driver function
    async def mock_get_sql_driver():
        return mock_sql_driver

    monkeypatch.setattr("pg_airman_mcp.server.get_sql_driver", mock_get_sql_driver)

    # Mock the SafeSqlDriver.execute_param_query method
    async def mock_execute_param_query(driver, query, params):
        if "d.description AS comment" in query and "d.objsubid = 0" in query:
            # Object-level comment query
            return [MockRow({"comment": "This is a test table"})]
        elif "column_name, data_type" in query:
            # Column metadata query
            return [
                MockRow(
                    {
                        "column_name": "id",
                        "data_type": "integer",
                        "is_nullable": "NO",
                        "column_default": "nextval('test_table_id_seq'::regclass)",
                    }
                ),
                MockRow(
                    {
                        "column_name": "name",
                        "data_type": "character varying",
                        "is_nullable": "YES",
                        "column_default": None,
                    }
                ),
            ]
        elif "a.attname AS column_name" in query:
            # Column comments query
            return [
                MockRow({"column_name": "id", "comment": "Primary key"}),
                MockRow({"column_name": "name", "comment": "User name"}),
            ]
        elif "constraint_name, constraint_type" in query:
            # Constraints query
            return [
                MockRow(
                    {
                        "constraint_name": "test_table_pkey",
                        "constraint_type": "PRIMARY KEY",
                        "column_name": "id",
                    }
                )
            ]
        elif "indexname, indexdef" in query:
            # Indexes query
            return [
                MockRow(
                    {
                        "indexname": "test_table_pkey",
                        "indexdef": (
                            "CREATE UNIQUE INDEX test_table_pkey "
                            "ON public.test_table USING btree (id)"
                        ),
                    }
                )
            ]
        return []

    monkeypatch.setattr(
        "pg_airman_mcp.server.SafeSqlDriver.execute_param_query",
        mock_execute_param_query,
    )

    # Call the function
    result = await get_object_details("public", "test_table", "table")

    # Verify the result structure
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], types.TextContent)

    # Parse the result to verify the object comment is included
    # Convert string representation back to dict safely
    result_data = ast.literal_eval(result[0].text)
    assert result_data["basic"]["comment"] == "This is a test table"

    # Check columns
    assert len(result_data["columns"]) == 2
    assert result_data["columns"][0]["comment"] == "Primary key"
    assert result_data["columns"][1]["comment"] == "User name"


@pytest.mark.asyncio
async def test_get_object_details_view_with_comment(mock_sql_driver, monkeypatch):
    """Test get_object_details for view with object-level comment."""

    async def mock_get_sql_driver():
        return mock_sql_driver

    monkeypatch.setattr("pg_airman_mcp.server.get_sql_driver", mock_get_sql_driver)

    async def mock_execute_param_query(driver, query, params):
        if "d.description AS comment" in query and "d.objsubid = 0" in query:
            return [MockRow({"comment": "This is a test view showing active users"})]
        elif "column_name, data_type" in query:
            return [
                MockRow(
                    {
                        "column_name": "user_id",
                        "data_type": "integer",
                        "is_nullable": "NO",
                        "column_default": None,
                    }
                ),
                MockRow(
                    {
                        "column_name": "username",
                        "data_type": "character varying",
                        "is_nullable": "YES",
                        "column_default": None,
                    }
                ),
            ]
        elif "a.attname AS column_name" in query:
            return [
                MockRow({"column_name": "user_id", "comment": "User identifier"}),
                MockRow({"column_name": "username", "comment": "Display name"}),
            ]
        elif "constraint_name, constraint_type" in query:
            return []
        elif "indexname, indexdef" in query:
            return []
        return []

    monkeypatch.setattr(
        "pg_airman_mcp.server.SafeSqlDriver.execute_param_query",
        mock_execute_param_query,
    )

    result = await get_object_details("public", "active_users_view", "view")
    result_data = ast.literal_eval(result[0].text)

    assert result_data["basic"]["comment"] == "This is a test view showing active users"
    assert len(result_data["columns"]) == 2
    assert result_data["columns"][0]["comment"] == "User identifier"
    assert result_data["columns"][1]["comment"] == "Display name"


@pytest.mark.asyncio
async def test_get_object_details_table_no_comment(mock_sql_driver, monkeypatch):
    """Table without object-level comment."""

    async def mock_get_sql_driver():
        return mock_sql_driver

    monkeypatch.setattr("pg_airman_mcp.server.get_sql_driver", mock_get_sql_driver)

    async def mock_execute_param_query(driver, query, params):
        if "d.description AS comment" in query and "d.objsubid = 0" in query:
            return [MockRow({"comment": None})]
        elif "column_name, data_type" in query:
            return [
                MockRow(
                    {
                        "column_name": "id",
                        "data_type": "integer",
                        "is_nullable": "NO",
                        "column_default": None,
                    }
                )
            ]
        elif "a.attname AS column_name" in query:
            return [MockRow({"column_name": "id", "comment": None})]
        elif "constraint_name, constraint_type" in query:
            return []
        elif "indexname, indexdef" in query:
            return []
        return []

    monkeypatch.setattr(
        "pg_airman_mcp.server.SafeSqlDriver.execute_param_query",
        mock_execute_param_query,
    )

    result = await get_object_details("public", "simple_table", "table")
    result_data = ast.literal_eval(result[0].text)
    assert result_data["basic"]["comment"] is None


@pytest.mark.asyncio
async def test_get_object_details_unsupported_type(mock_sql_driver, monkeypatch):
    """Unsupported object type returns error response."""

    async def mock_get_sql_driver():
        return mock_sql_driver

    monkeypatch.setattr("pg_airman_mcp.server.get_sql_driver", mock_get_sql_driver)

    result = await get_object_details("public", "test_object", "unsupported_type")
    assert "Error: Unsupported object type" in result[0].text
