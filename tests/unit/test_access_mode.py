import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pg_airman_mcp.server import AccessMode, get_sql_driver
from pg_airman_mcp.sql.safe_sql import SafeSqlDriver
from pg_airman_mcp.sql.sql_driver import DbConnPool, SqlDriver


@pytest.fixture
def mock_db_connection():
    """Mock database connection pool."""
    conn = MagicMock(spec=DbConnPool)
    conn.is_valid = True
    return conn


@pytest.mark.parametrize(
    "access_mode,expected_driver_type",
    [
        (AccessMode.UNRESTRICTED, SqlDriver),
        (AccessMode.RESTRICTED, SafeSqlDriver),
    ],
)
@pytest.mark.asyncio
async def test_get_sql_driver_returns_correct_driver(
    access_mode, expected_driver_type, mock_db_connection
):
    """Test that get_sql_driver returns the correct driver type based on access mode."""
    with (
        patch("pg_airman_mcp.server.current_access_mode", access_mode),
        patch("pg_airman_mcp.server.db_connection", mock_db_connection),
    ):
        driver = await get_sql_driver()
        assert isinstance(driver, expected_driver_type)

        # When in RESTRICTED mode, verify timeout is set
        if access_mode == AccessMode.RESTRICTED:
            assert isinstance(driver, SafeSqlDriver)
            assert driver.timeout == 30


@pytest.mark.asyncio
async def test_get_sql_driver_sets_timeout_in_restricted_mode(mock_db_connection):
    """Test that get_sql_driver sets the timeout in restricted mode."""
    with (
        patch("pg_airman_mcp.server.current_access_mode", AccessMode.RESTRICTED),
        patch("pg_airman_mcp.server.db_connection", mock_db_connection),
    ):
        driver = await get_sql_driver()
        assert isinstance(driver, SafeSqlDriver)
        assert driver.timeout == 30
        assert hasattr(driver, "sql_driver")


@pytest.mark.asyncio
async def test_get_sql_driver_in_unrestricted_mode_no_timeout(mock_db_connection):
    """Test that get_sql_driver in unrestricted mode is a regular SqlDriver."""
    with (
        patch("pg_airman_mcp.server.current_access_mode", AccessMode.UNRESTRICTED),
        patch("pg_airman_mcp.server.db_connection", mock_db_connection),
    ):
        driver = await get_sql_driver()
        assert isinstance(driver, SqlDriver)
        assert not hasattr(driver, "timeout")


@pytest.mark.asyncio
async def test_command_line_parsing():
    """Test that command-line arguments correctly set the access mode."""
    import sys

    from pg_airman_mcp.server import main

    # Mock sys.argv and asyncio.run
    original_argv = sys.argv
    original_run = asyncio.run

    try:
        # Test with --access-mode=restricted
        sys.argv = [
            "pg_airman_mcp",
            "postgresql://user:password@localhost/db",
            "--access-mode=restricted",
        ]
        asyncio.run = AsyncMock()

        with (
            patch("pg_airman_mcp.server.current_access_mode", AccessMode.UNRESTRICTED),
            patch("pg_airman_mcp.server.db_connection.pool_connect", AsyncMock()),
            patch("pg_airman_mcp.server.mcp.run_stdio_async", AsyncMock()),
            patch("pg_airman_mcp.server.shutdown", AsyncMock()),
        ):
            # Reset the current_access_mode to UNRESTRICTED
            import pg_airman_mcp.server

            pg_airman_mcp.server.current_access_mode = AccessMode.UNRESTRICTED

            # Run main (partially mocked to avoid actual connection,
            # and to control shutdown behavior)
            with patch(
                "pg_airman_mcp.server.shutdown_event.is_set", MagicMock()
            ) as mock_shutdown:
                mock_shutdown.side_effect = [False, True]
                try:
                    await main()
                except SystemExit:
                    pass

            # Verify the mode was changed to RESTRICTED
            assert pg_airman_mcp.server.current_access_mode == AccessMode.RESTRICTED

    finally:
        # Restore original values
        sys.argv = original_argv
        asyncio.run = original_run
