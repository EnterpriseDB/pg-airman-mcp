"""Integration tests for session token tracing: SET application_name.

Tests verify that SET application_name (session-level, via autocommit)
correctly sets the value visible in current_setting during the query,
and that an outside observer querying pg_stat_activity sees the value
on the idle pooled connection after the query completes — the actual
POS-96 promise.
"""

import logging

import psycopg
import pytest
import pytest_asyncio

from pg_airman_mcp.sql import DbConnPool, SqlDriver, tracing_application_name
from pg_airman_mcp.sql.safe_sql import SafeSqlDriver

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


@pytest_asyncio.fixture
async def admin_conn_factory(test_postgres_connection_string):
    """Factory for opening fresh admin connections to probe pg_stat_activity."""
    connection_string, _ = test_postgres_connection_string

    async def _open():
        return await psycopg.AsyncConnection.connect(connection_string)

    return _open


class TestTracingSetApplicationName:
    """Verify SET application_name via tracing_application_name ContextVar."""

    @pytest.mark.asyncio
    async def test_application_name_visible_during_query(self, tracing_pool):
        """SET application_name visible via current_setting during query."""
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
    async def test_application_name_visible_on_idle_pool_connection(
        self, tracing_pool, admin_conn_factory
    ):
        """Once a tool call finishes, the pooled connection sits idle and
        pg_stat_activity shows its application_name — visible to operators.

        This is the actual POS-96 promise: session tracing is visible on
        idle pool connections from an outside observer. Uses a separate
        admin connection to query pg_stat_activity.
        """
        driver = SqlDriver(conn=tracing_pool)

        reset_token = tracing_application_name.set("idle-trace-789")
        try:
            rows = await driver.execute_query("SELECT pg_backend_pid() AS pid")
        finally:
            tracing_application_name.reset(reset_token)

        assert rows is not None and len(rows) == 1
        backend_pid = rows[0].cells["pid"]

        admin = await admin_conn_factory()
        try:
            async with admin.cursor() as cur:
                await cur.execute(
                    "SELECT state, application_name FROM pg_stat_activity "
                    "WHERE pid = %s",
                    (backend_pid,),
                )
                row = await cur.fetchone()
        finally:
            await admin.close()

        assert row is not None, f"backend pid {backend_pid} not found"
        state, app_name = row
        assert state == "idle"
        assert app_name == "idle-trace-789"

    @pytest.mark.asyncio
    async def test_application_name_visible_in_restricted_mode(
        self, tracing_pool, admin_conn_factory
    ):
        """Restricted mode (SafeSqlDriver, force_readonly=True) also persists
        the tracing token on idle pool connections.

        SafeSqlDriver wraps SqlDriver and always calls execute_query with
        force_readonly=True, which adds a BEGIN TRANSACTION READ ONLY /
        ROLLBACK wrapper around the query. The SET application_name is
        issued BEFORE that BEGIN via autocommit, so the subsequent
        ROLLBACK should not affect session-level app_name.
        """
        safe_driver = SafeSqlDriver(sql_driver=SqlDriver(conn=tracing_pool))

        reset_token = tracing_application_name.set("restricted-trace-xyz")
        try:
            rows = await safe_driver.execute_query("SELECT pg_backend_pid() AS pid")
        finally:
            tracing_application_name.reset(reset_token)

        assert rows is not None and len(rows) == 1
        backend_pid = rows[0].cells["pid"]

        admin = await admin_conn_factory()
        try:
            async with admin.cursor() as cur:
                await cur.execute(
                    "SELECT state, application_name FROM pg_stat_activity "
                    "WHERE pid = %s",
                    (backend_pid,),
                )
                row = await cur.fetchone()
        finally:
            await admin.close()

        assert row is not None, f"backend pid {backend_pid} not found"
        state, app_name = row
        assert state == "idle"
        assert app_name == "restricted-trace-xyz"

    @pytest.mark.asyncio
    async def test_no_set_without_contextvar(self, tracing_pool):
        """Without ContextVar set, application_name is not overridden."""
        driver = SqlDriver(conn=tracing_pool)

        rows = await driver.execute_query(
            "SELECT current_setting('application_name') AS application_name"
        )

        assert rows is not None
        assert len(rows) == 1
        assert rows[0].cells["application_name"] != "trace-token-123"

    @pytest.mark.asyncio
    async def test_composed_token_and_purpose(self, tracing_pool):
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
