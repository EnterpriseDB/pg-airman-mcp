"""Integration tests for purpose logging: application_name in pg_stat_activity."""

import logging

import pytest
import pytest_asyncio

from pg_airman_mcp.sql import DbConnPool, SqlDriver

logger = logging.getLogger(__name__)


@pytest_asyncio.fixture
async def connection_with_purpose(test_postgres_connection_string):
    """Create a connection pool with purpose set via application_name."""
    connection_string, version = test_postgres_connection_string
    logger.info("Using PostgreSQL version: %s", version)

    # Format: airman:<purpose> (matches server.py format)
    purpose = "airman:test-purpose"
    separator = "&" if "?" in connection_string else "?"
    url_with_purpose = f"{connection_string}{separator}application_name={purpose}"

    pool = DbConnPool()
    await pool.pool_connect(url_with_purpose)
    try:
        yield pool, purpose
    finally:
        await pool.close()


@pytest_asyncio.fixture
async def connection_without_purpose(test_postgres_connection_string):
    """Create a connection pool without purpose."""
    connection_string, _version = test_postgres_connection_string

    pool = DbConnPool()
    await pool.pool_connect(connection_string)
    try:
        yield pool
    finally:
        await pool.close()


class TestPurposeApplicationName:
    """Verify application_name appears in pg_stat_activity."""

    @pytest.mark.asyncio
    async def test_application_name_set_in_pg_stat_activity(
        self, connection_with_purpose
    ):
        """Purpose value is visible as application_name in pg_stat_activity."""
        pool, purpose = connection_with_purpose
        driver = SqlDriver(conn=pool)

        rows = await driver.execute_query(
            "SELECT application_name FROM pg_stat_activity WHERE pid = pg_backend_pid()"
        )

        assert rows is not None
        assert len(rows) == 1
        assert rows[0].cells["application_name"] == purpose

    @pytest.mark.asyncio
    async def test_application_name_default_without_purpose(
        self, connection_without_purpose
    ):
        """Without purpose, application_name is not our purpose value."""
        pool = connection_without_purpose
        driver = SqlDriver(conn=pool)

        rows = await driver.execute_query(
            "SELECT application_name FROM pg_stat_activity WHERE pid = pg_backend_pid()"
        )

        assert rows is not None
        assert len(rows) == 1
        assert rows[0].cells["application_name"] != "airman:test-purpose"

    @pytest.mark.asyncio
    async def test_application_name_persists_across_queries(
        self, connection_with_purpose
    ):
        """application_name persists across multiple queries on same pool."""
        pool, purpose = connection_with_purpose
        driver = SqlDriver(conn=pool)

        # Run several queries
        for _ in range(3):
            await driver.execute_query("SELECT 1")

        # Check application_name is still set
        rows = await driver.execute_query(
            "SELECT application_name FROM pg_stat_activity WHERE pid = pg_backend_pid()"
        )

        assert rows is not None
        assert rows[0].cells["application_name"] == purpose

    @pytest.mark.asyncio
    async def test_application_name_with_hyphenated_purpose(
        self, test_postgres_connection_string
    ):
        """Hyphenated purpose value works correctly in connection URL."""
        connection_string, _version = test_postgres_connection_string
        # Format: airman:<purpose> (matches server.py format)
        purpose = "airman:credit-scoring-v2"
        separator = "&" if "?" in connection_string else "?"
        url = f"{connection_string}{separator}application_name={purpose}"

        pool = DbConnPool()
        await pool.pool_connect(url)
        try:
            driver = SqlDriver(conn=pool)
            rows = await driver.execute_query(
                "SELECT application_name FROM pg_stat_activity "
                "WHERE pid = pg_backend_pid()"
            )

            assert rows is not None
            assert rows[0].cells["application_name"] == purpose
        finally:
            await pool.close()
