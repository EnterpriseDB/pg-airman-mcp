"""Integration tests for session token tracing: SET LOCAL application_name."""

import logging

import pytest
import pytest_asyncio

from pg_airman_mcp.sql import DbConnPool, SqlDriver, tracing_application_name

logger = logging.getLogger(__name__)


@pytest_asyncio.fixture
async def tracing_pool(test_postgres_connection_string):
    """Create a plain connection pool for tracing tests."""
    connection_string, version = test_postgres_connection_string
    logger.info("Using PostgreSQL version: %s", version)

    pool = DbConnPool()
    await pool.pool_connect(connection_string)
    try:
        yield pool
    finally:
        await pool.close()


class TestTracingSetLocal:
    """Verify SET LOCAL application_name via tracing_application_name ContextVar."""

    @pytest.mark.asyncio
    async def test_set_local_application_name_visible(self, tracing_pool):
        """SET LOCAL application_name visible via current_setting."""
        driver = SqlDriver(conn=tracing_pool)

        reset_token = tracing_application_name.set("trace-token-123")
        try:
            rows = await driver.execute_query(
                "SELECT current_setting('application_name') AS application_name"
            )

            assert rows is not None
            assert len(rows) == 1
            assert rows[0].cells["application_name"] == "trace-token-123"
        finally:
            tracing_application_name.reset(reset_token)

    @pytest.mark.asyncio
    async def test_no_set_local_without_contextvar(self, tracing_pool):
        """Without ContextVar set, application_name is not overridden."""
        driver = SqlDriver(conn=tracing_pool)

        rows = await driver.execute_query(
            "SELECT current_setting('application_name') AS application_name"
        )

        assert rows is not None
        assert len(rows) == 1
        assert rows[0].cells["application_name"] != "trace-token-123"

    @pytest.mark.asyncio
    async def test_set_local_with_composed_token_and_purpose(self, tracing_pool):
        """Composed airman:<purpose>/<session> value is visible via current_setting."""
        driver = SqlDriver(conn=tracing_pool)
        composed_value = "airman:credit-scoring/abc12345"

        reset_token = tracing_application_name.set(composed_value)
        try:
            rows = await driver.execute_query(
                "SELECT current_setting('application_name') AS application_name"
            )

            assert rows is not None
            assert len(rows) == 1
            assert rows[0].cells["application_name"] == composed_value
        finally:
            tracing_application_name.reset(reset_token)
