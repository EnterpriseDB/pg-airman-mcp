"""Unit tests for session token tracing (POS-93, POS-96).

Tests cover:
- ServerSettings.tracing field defaults and env var
- --tracing / --no-tracing CLI argument handling
- AirmanFastMCP.call_tool() token extraction and ContextVar management
- create_mcp_server() class selection based on tracing flag
- SET application_name injection in sql_driver._execute_with_connection()
  (session-level via autocommit, not SET LOCAL)
- Null sessionId handling (fallback to generated UUID)
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from mcp.server.fastmcp import FastMCP

from pg_airman_mcp.server import AirmanFastMCP, ServerSettings, create_mcp_server
from pg_airman_mcp.sql import DbConnPool, SqlDriver, tracing_application_name


class TestTracingSettings:
    """Test ServerSettings.tracing field."""

    def test_default_is_false(self):
        """Default tracing is False."""
        s = ServerSettings()
        assert s.tracing is False

    def test_tracing_true_accepted(self):
        """Explicit True is accepted."""
        s = ServerSettings(tracing=True)
        assert s.tracing is True

    def test_env_var(self, monkeypatch):
        """AIRMAN_MCP_TRACING env var enables tracing."""
        monkeypatch.setenv("AIRMAN_MCP_TRACING", "true")
        s = ServerSettings()
        assert s.tracing is True


class TestTracingCLIArg:
    """Test --tracing / --no-tracing CLI argument in main()."""

    @pytest.mark.asyncio
    async def test_tracing_cli_sets_true(self):
        """--tracing CLI arg sets settings.tracing to True."""
        from pg_airman_mcp.server import main

        with (
            patch("pg_airman_mcp.server.ServerSettings") as mock_settings,
            patch("pg_airman_mcp.server.create_mcp_server") as mock_create,
            patch("pg_airman_mcp.server.db_connection") as mock_db,
            patch(
                "sys.argv",
                ["server.py", "postgresql://test", "--tracing"],
            ),
            patch("signal.signal"),
        ):
            mock_settings_instance = MagicMock()
            mock_settings_instance.database_url = "postgresql://test"
            mock_settings_instance.access_mode = "unrestricted"
            mock_settings_instance.transport = "stdio"
            mock_settings_instance.auth_enabled = False
            mock_settings_instance.purpose = None
            mock_settings_instance.tracing = False
            mock_settings_instance.determine_server_url = MagicMock(return_value=None)
            mock_settings_instance.get_required_scopes = MagicMock(return_value=[])
            mock_settings.return_value = mock_settings_instance

            mock_mcp = MagicMock()
            mock_mcp.add_tool = MagicMock()
            mock_mcp.list_tools = AsyncMock(return_value=[])
            mock_mcp.run_stdio_async = AsyncMock()
            mock_create.return_value = mock_mcp

            mock_db.pool_connect = AsyncMock()
            mock_db.close = AsyncMock()

            with patch("pg_airman_mcp.server.shutdown_event") as mock_event:
                mock_event.is_set = MagicMock(side_effect=[False, True])

                with pytest.raises(SystemExit) as exc:
                    await main()
                assert exc.value.code == 0

            assert mock_settings_instance.tracing is True

    @pytest.mark.asyncio
    async def test_no_tracing_cli_sets_false(self):
        """--no-tracing CLI arg sets settings.tracing to False."""
        from pg_airman_mcp.server import main

        with (
            patch("pg_airman_mcp.server.ServerSettings") as mock_settings,
            patch("pg_airman_mcp.server.create_mcp_server") as mock_create,
            patch("pg_airman_mcp.server.db_connection") as mock_db,
            patch(
                "sys.argv",
                ["server.py", "postgresql://test", "--no-tracing"],
            ),
            patch("signal.signal"),
        ):
            mock_settings_instance = MagicMock()
            mock_settings_instance.database_url = "postgresql://test"
            mock_settings_instance.access_mode = "unrestricted"
            mock_settings_instance.transport = "stdio"
            mock_settings_instance.auth_enabled = False
            mock_settings_instance.purpose = None
            mock_settings_instance.tracing = False
            mock_settings_instance.determine_server_url = MagicMock(return_value=None)
            mock_settings_instance.get_required_scopes = MagicMock(return_value=[])
            mock_settings.return_value = mock_settings_instance

            mock_mcp = MagicMock()
            mock_mcp.add_tool = MagicMock()
            mock_mcp.list_tools = AsyncMock(return_value=[])
            mock_mcp.run_stdio_async = AsyncMock()
            mock_create.return_value = mock_mcp

            mock_db.pool_connect = AsyncMock()
            mock_db.close = AsyncMock()

            with patch("pg_airman_mcp.server.shutdown_event") as mock_event:
                mock_event.is_set = MagicMock(side_effect=[False, True])

                with pytest.raises(SystemExit) as exc:
                    await main()
                assert exc.value.code == 0

            assert mock_settings_instance.tracing is False

    @pytest.mark.asyncio
    async def test_tracing_passed_to_create_mcp_server(self):
        """--tracing flag is forwarded to create_mcp_server()."""
        from pg_airman_mcp.server import main

        with (
            patch("pg_airman_mcp.server.ServerSettings") as mock_settings,
            patch("pg_airman_mcp.server.create_mcp_server") as mock_create,
            patch("pg_airman_mcp.server.db_connection") as mock_db,
            patch(
                "sys.argv",
                ["server.py", "postgresql://test", "--tracing"],
            ),
            patch("signal.signal"),
        ):
            mock_settings_instance = MagicMock()
            mock_settings_instance.database_url = "postgresql://test"
            mock_settings_instance.access_mode = "unrestricted"
            mock_settings_instance.transport = "stdio"
            mock_settings_instance.auth_enabled = False
            mock_settings_instance.purpose = None
            mock_settings_instance.tracing = False
            mock_settings_instance.determine_server_url = MagicMock(return_value=None)
            mock_settings_instance.get_required_scopes = MagicMock(return_value=[])
            mock_settings.return_value = mock_settings_instance

            mock_mcp = MagicMock()
            mock_mcp.add_tool = MagicMock()
            mock_mcp.list_tools = AsyncMock(return_value=[])
            mock_mcp.run_stdio_async = AsyncMock()
            mock_create.return_value = mock_mcp

            mock_db.pool_connect = AsyncMock()
            mock_db.close = AsyncMock()

            with patch("pg_airman_mcp.server.shutdown_event") as mock_event:
                mock_event.is_set = MagicMock(side_effect=[False, True])

                with pytest.raises(SystemExit) as exc:
                    await main()
                assert exc.value.code == 0

            # Verify create_mcp_server was called with tracing=True
            call_kwargs = mock_create.call_args[1]
            assert call_kwargs["tracing"] is True


class TestAirmanFastMCPCallTool:
    """Test AirmanFastMCP.call_tool() token extraction and ContextVar."""

    @pytest.mark.asyncio
    async def test_extracts_session_token_from_meta(self):
        """Session token is extracted from _meta.sessionId."""
        server = AirmanFastMCP("test-server", fallback_token="fallback-uuid")

        mock_meta = MagicMock()
        mock_meta.model_extra = {"sessionId": "abc123"}
        mock_request_context = MagicMock()
        mock_request_context.meta = mock_meta
        mock_ctx = MagicMock()
        mock_ctx.request_context = mock_request_context

        captured: dict[str, str | None] = {}

        async def capture_contextvar(*args, **kwargs):
            captured["app_name"] = tracing_application_name.get()
            return []

        with (
            patch.object(AirmanFastMCP, "get_context", return_value=mock_ctx),
            patch.object(FastMCP, "call_tool", new_callable=AsyncMock) as mock_super,
        ):
            mock_super.side_effect = capture_contextvar
            await server.call_tool("test_tool", {})

        # Format: airman:_/<session-short> (no purpose, 8-char session)
        assert captured["app_name"] == "airman:_/abc123"

    @pytest.mark.asyncio
    async def test_fallback_token_when_meta_is_none(self):
        """Fallback token is used when meta is None."""
        server = AirmanFastMCP("test-server", fallback_token="fallback-uuid")

        mock_request_context = MagicMock()
        mock_request_context.meta = None
        mock_ctx = MagicMock()
        mock_ctx.request_context = mock_request_context

        captured: dict[str, str | None] = {}

        async def capture_contextvar(*args, **kwargs):
            captured["app_name"] = tracing_application_name.get()
            return []

        with (
            patch.object(AirmanFastMCP, "get_context", return_value=mock_ctx),
            patch.object(FastMCP, "call_tool", new_callable=AsyncMock) as mock_super,
        ):
            mock_super.side_effect = capture_contextvar
            await server.call_tool("test_tool", {})

        # Format: airman:_/<session-short> (8-char truncation of fallback-uuid)
        assert captured["app_name"] == "airman:_/fallback"

    @pytest.mark.asyncio
    async def test_fallback_token_when_session_token_key_missing(self):
        """Fallback token is used when model_extra has no sessionId."""
        server = AirmanFastMCP("test-server", fallback_token="fallback-uuid")

        mock_meta = MagicMock()
        mock_meta.model_extra = {}
        mock_request_context = MagicMock()
        mock_request_context.meta = mock_meta
        mock_ctx = MagicMock()
        mock_ctx.request_context = mock_request_context

        captured: dict[str, str | None] = {}

        async def capture_contextvar(*args, **kwargs):
            captured["app_name"] = tracing_application_name.get()
            return []

        with (
            patch.object(AirmanFastMCP, "get_context", return_value=mock_ctx),
            patch.object(FastMCP, "call_tool", new_callable=AsyncMock) as mock_super,
        ):
            mock_super.side_effect = capture_contextvar
            await server.call_tool("test_tool", {})

        # Format: airman:_/<session-short> (8-char truncation of fallback-uuid)
        assert captured["app_name"] == "airman:_/fallback"

    @pytest.mark.asyncio
    async def test_composes_token_with_purpose(self):
        """app_name is airman:<purpose>/<session-short> when purpose is set."""
        server = AirmanFastMCP(
            "test-server", purpose="my-app", fallback_token="fallback-uuid"
        )

        mock_meta = MagicMock()
        mock_meta.model_extra = {"sessionId": "abc123"}
        mock_request_context = MagicMock()
        mock_request_context.meta = mock_meta
        mock_ctx = MagicMock()
        mock_ctx.request_context = mock_request_context

        captured: dict[str, str | None] = {}

        async def capture_contextvar(*args, **kwargs):
            captured["app_name"] = tracing_application_name.get()
            return []

        with (
            patch.object(AirmanFastMCP, "get_context", return_value=mock_ctx),
            patch.object(FastMCP, "call_tool", new_callable=AsyncMock) as mock_super,
        ):
            mock_super.side_effect = capture_contextvar
            await server.call_tool("test_tool", {})

        # Format: airman:<purpose>/<session-short>
        assert captured["app_name"] == "airman:my-app/abc123"

    @pytest.mark.asyncio
    async def test_token_only_when_purpose_is_none(self):
        """app_name is airman:_/<session-short> when purpose is None."""
        server = AirmanFastMCP(
            "test-server", purpose=None, fallback_token="fallback-uuid"
        )

        mock_meta = MagicMock()
        mock_meta.model_extra = {"sessionId": "abc123"}
        mock_request_context = MagicMock()
        mock_request_context.meta = mock_meta
        mock_ctx = MagicMock()
        mock_ctx.request_context = mock_request_context

        captured: dict[str, str | None] = {}

        async def capture_contextvar(*args, **kwargs):
            captured["app_name"] = tracing_application_name.get()
            return []

        with (
            patch.object(AirmanFastMCP, "get_context", return_value=mock_ctx),
            patch.object(FastMCP, "call_tool", new_callable=AsyncMock) as mock_super,
        ):
            mock_super.side_effect = capture_contextvar
            await server.call_tool("test_tool", {})

        # Format: airman:_/<session-short> (no purpose)
        assert captured["app_name"] == "airman:_/abc123"

    @pytest.mark.asyncio
    async def test_contextvar_set_during_tool_execution(self):
        """ContextVar is set to the expected value during super().call_tool()."""
        server = AirmanFastMCP("test-server", fallback_token="fallback-uuid")

        mock_meta = MagicMock()
        mock_meta.model_extra = {"sessionId": "traced-token"}
        mock_request_context = MagicMock()
        mock_request_context.meta = mock_meta
        mock_ctx = MagicMock()
        mock_ctx.request_context = mock_request_context

        captured: dict[str, str | None] = {}

        async def capture_contextvar(*args, **kwargs):
            captured["app_name"] = tracing_application_name.get()
            return []

        with (
            patch.object(AirmanFastMCP, "get_context", return_value=mock_ctx),
            patch.object(FastMCP, "call_tool", new_callable=AsyncMock) as mock_super,
        ):
            mock_super.side_effect = capture_contextvar
            await server.call_tool("test_tool", {})

        # Format: airman:_/<session-short> (8-char truncation of traced-token)
        assert captured["app_name"] == "airman:_/traced-t"

    @pytest.mark.asyncio
    async def test_contextvar_reset_after_execution(self):
        """ContextVar is reset to None after call_tool completes."""
        server = AirmanFastMCP("test-server", fallback_token="fallback-uuid")

        mock_meta = MagicMock()
        mock_meta.model_extra = {"sessionId": "abc123"}
        mock_request_context = MagicMock()
        mock_request_context.meta = mock_meta
        mock_ctx = MagicMock()
        mock_ctx.request_context = mock_request_context

        with (
            patch.object(AirmanFastMCP, "get_context", return_value=mock_ctx),
            patch.object(FastMCP, "call_tool", new_callable=AsyncMock) as mock_super,
        ):
            mock_super.return_value = []
            await server.call_tool("test_tool", {})

        assert tracing_application_name.get() is None

    @pytest.mark.asyncio
    async def test_contextvar_reset_on_exception(self):
        """ContextVar is reset even when super().call_tool() raises."""
        server = AirmanFastMCP("test-server", fallback_token="fallback-uuid")

        mock_meta = MagicMock()
        mock_meta.model_extra = {"sessionId": "abc123"}
        mock_request_context = MagicMock()
        mock_request_context.meta = mock_meta
        mock_ctx = MagicMock()
        mock_ctx.request_context = mock_request_context

        with (
            patch.object(AirmanFastMCP, "get_context", return_value=mock_ctx),
            patch.object(FastMCP, "call_tool", new_callable=AsyncMock) as mock_super,
        ):
            mock_super.side_effect = RuntimeError("tool failed")
            with pytest.raises(RuntimeError, match="tool failed"):
                await server.call_tool("test_tool", {})

        assert tracing_application_name.get() is None

    @pytest.mark.asyncio
    async def test_fallback_token_when_get_context_raises(self):
        """Fallback token is used when get_context() raises."""
        server = AirmanFastMCP("test-server", fallback_token="fallback-uuid")

        captured: dict[str, str | None] = {}

        async def capture_contextvar(*args, **kwargs):
            captured["app_name"] = tracing_application_name.get()
            return []

        with (
            patch.object(
                AirmanFastMCP,
                "get_context",
                side_effect=RuntimeError("no context"),
            ),
            patch.object(FastMCP, "call_tool", new_callable=AsyncMock) as mock_super,
        ):
            mock_super.side_effect = capture_contextvar
            await server.call_tool("test_tool", {})

        # Format: airman:_/<session-short> (8-char truncation of fallback-uuid)
        assert captured["app_name"] == "airman:_/fallback"

    @pytest.mark.asyncio
    async def test_null_session_id_uses_fallback(self):
        """Fallback token is used when sessionId is explicitly None."""
        server = AirmanFastMCP("test-server", fallback_token="fallback-uuid")

        mock_meta = MagicMock()
        mock_meta.model_extra = {"sessionId": None}
        mock_request_context = MagicMock()
        mock_request_context.meta = mock_meta
        mock_ctx = MagicMock()
        mock_ctx.request_context = mock_request_context

        captured: dict[str, str | None] = {}

        async def capture_contextvar(*args, **kwargs):
            captured["app_name"] = tracing_application_name.get()
            return []

        with (
            patch.object(AirmanFastMCP, "get_context", return_value=mock_ctx),
            patch.object(FastMCP, "call_tool", new_callable=AsyncMock) as mock_super,
        ):
            mock_super.side_effect = capture_contextvar
            await server.call_tool("test_tool", {})

        # sessionId=None should fall back, not produce "airman:_/None"
        assert captured["app_name"] == "airman:_/fallback"

    def test_default_fallback_token_is_uuid(self):
        """Fallback token is a UUID when not explicitly provided."""
        import uuid

        server = AirmanFastMCP("test-server")
        # Verify it parses as a valid UUID
        parsed = uuid.UUID(server._fallback_token)  # type: ignore[reportPrivateUsage]
        assert str(parsed) == server._fallback_token  # type: ignore[reportPrivateUsage]


class TestCreateMcpServerTracing:
    """Test create_mcp_server() class selection based on tracing flag."""

    def test_tracing_true_returns_airman_instance(self):
        """tracing=True creates an AirmanFastMCP instance."""
        server = create_mcp_server(tracing=True)
        assert isinstance(server, AirmanFastMCP)

    def test_tracing_false_returns_plain_fastmcp(self):
        """tracing=False creates a plain FastMCP (not AirmanFastMCP)."""
        server = create_mcp_server(tracing=False)
        assert type(server) is FastMCP

    def test_tracing_with_purpose_passes_purpose(self):
        """Purpose is stored on the AirmanFastMCP instance."""
        server = create_mcp_server(tracing=True, purpose="my-app")
        assert isinstance(server, AirmanFastMCP)
        assert server._purpose == "my-app"  # type: ignore[reportPrivateUsage]

    def test_tracing_without_purpose_has_none(self):
        """Purpose defaults to None when not provided."""
        server = create_mcp_server(tracing=True)
        assert isinstance(server, AirmanFastMCP)
        assert server._purpose is None  # type: ignore[reportPrivateUsage]

    def test_tracing_logs_info(self):
        """Tracing enabled logs an info message."""
        with patch("pg_airman_mcp.server.logger") as mock_logger:
            create_mcp_server(tracing=True, purpose="my-app")

        log_messages = [str(call) for call in mock_logger.info.call_args_list]
        assert any(
            "tracing" in msg.lower() and "AirmanFastMCP" in msg for msg in log_messages
        ), f"Expected tracing log message, got: {log_messages}"


class TestSetApplicationName:
    """Test SET application_name injection in sql_driver.

    The SET runs on the connection via autocommit (not on the cursor,
    not inside a transaction) so it persists as a session-level setting.
    """

    @pytest.fixture(autouse=True)
    def _reset_contextvar(self):
        """Ensure tracing ContextVar is reset after each test."""
        yield
        # Reset to default (None)
        token = tracing_application_name.set(None)
        tracing_application_name.reset(token)

    def _make_mock_pool(self):
        """Create a mock DbConnPool with cursor that avoids nextset loops."""
        mock_cursor = MagicMock()  # NOT AsyncMock — nextset() infinite loop!
        mock_cursor.execute = AsyncMock()
        mock_cursor.fetchall = AsyncMock(return_value=[])
        mock_cursor.nextset = MagicMock(return_value=False)
        mock_cursor.description = None
        mock_cursor.__aenter__ = AsyncMock(return_value=mock_cursor)
        mock_cursor.__aexit__ = AsyncMock(return_value=False)

        mock_conn = AsyncMock()
        mock_conn.cursor = MagicMock(return_value=mock_cursor)
        mock_conn.execute = AsyncMock()
        mock_conn.set_autocommit = AsyncMock()
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)

        mock_pool_obj = AsyncMock()
        mock_pool_obj.connection = MagicMock(return_value=mock_conn)

        pool = DbConnPool()
        pool.pool = mock_pool_obj  # satisfy pool_connect() truthy check
        pool._is_valid = True  # type: ignore[reportPrivateUsage]  # skip reconnection logic

        return pool, mock_conn, mock_cursor

    @pytest.mark.asyncio
    async def test_set_application_name_when_contextvar_set(self):
        """SET application_name is issued on connection when ContextVar is set."""
        pool, mock_conn, _mock_cursor = self._make_mock_pool()
        driver = SqlDriver(conn=pool)

        tracing_application_name.set("test-token")

        await driver.execute_query("SELECT 1")

        # SET runs on connection (not cursor) via autocommit
        conn_execute_calls = [str(call) for call in mock_conn.execute.call_args_list]
        assert any("SET application_name" in call for call in conn_execute_calls), (
            f"Expected SET application_name on connection, got: {conn_execute_calls}"
        )

    @pytest.mark.asyncio
    async def test_autocommit_enabled_for_set(self):
        """Autocommit is enabled before SET and disabled after."""
        pool, mock_conn, _mock_cursor = self._make_mock_pool()
        driver = SqlDriver(conn=pool)

        tracing_application_name.set("test-token")

        await driver.execute_query("SELECT 1")

        # Check autocommit calls: True before SET, False after
        autocommit_calls = [
            call[0][0] for call in mock_conn.set_autocommit.call_args_list
        ]
        assert autocommit_calls == [True, False], (
            f"Expected [True, False] autocommit sequence, got: {autocommit_calls}"
        )

    @pytest.mark.asyncio
    async def test_no_set_when_contextvar_is_none(self):
        """No SET is issued when ContextVar is None (default)."""
        pool, mock_conn, _mock_cursor = self._make_mock_pool()
        driver = SqlDriver(conn=pool)

        # ContextVar default is None — no need to set it
        await driver.execute_query("SELECT 1")

        conn_execute_calls = [str(call) for call in mock_conn.execute.call_args_list]
        assert not any("SET application_name" in call for call in conn_execute_calls), (
            f"Unexpected SET application_name in: {conn_execute_calls}"
        )
        # Autocommit should not be toggled
        assert mock_conn.set_autocommit.call_count == 0

    @pytest.mark.asyncio
    async def test_set_uses_literal_for_safety(self):
        """SET uses psycopg SQL.Literal for safe parameterization."""
        pool, mock_conn, _mock_cursor = self._make_mock_pool()
        driver = SqlDriver(conn=pool)

        tracing_application_name.set("token-with-special")

        await driver.execute_query("SELECT 1")

        set_calls = [
            call
            for call in mock_conn.execute.call_args_list
            if "SET application_name" in str(call)
        ]
        assert len(set_calls) == 1
        # The argument should be a psycopg sql.Composed object, not a string
        sql_arg = set_calls[0][0][0]
        assert not isinstance(sql_arg, str), (
            "SET should use psycopg sql.SQL, not a raw string"
        )
