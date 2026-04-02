"""Unit tests for purpose logging: validator, CLI arg, connection URL append."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from pg_airman_mcp.server import ServerSettings


class TestPurposeValidator:
    """Test purpose field validation on ServerSettings."""

    def test_valid_single_char(self):
        """Single character purpose is valid."""
        s = ServerSettings(purpose="a")
        assert s.purpose == "a"

    def test_valid_single_digit(self):
        """Single digit purpose is valid."""
        s = ServerSettings(purpose="1")
        assert s.purpose == "1"

    def test_valid_lowercase_alpha(self):
        """Lowercase alphabetic purpose is valid."""
        s = ServerSettings(purpose="creditscoring")
        assert s.purpose == "creditscoring"

    def test_valid_with_hyphens(self):
        """Lowercase with hyphens is valid."""
        s = ServerSettings(purpose="credit-scoring")
        assert s.purpose == "credit-scoring"

    def test_valid_alphanumeric_with_hyphens(self):
        """Alphanumeric with hyphens is valid."""
        s = ServerSettings(purpose="app-v2-prod")
        assert s.purpose == "app-v2-prod"

    def test_valid_digits_only(self):
        """All-digit purpose is valid."""
        s = ServerSettings(purpose="123")
        assert s.purpose == "123"

    def test_valid_consecutive_hyphens(self):
        """Consecutive hyphens between valid chars are accepted."""
        s = ServerSettings(purpose="a--b")
        assert s.purpose == "a--b"

    def test_valid_none(self):
        """None purpose is valid (feature disabled)."""
        s = ServerSettings(purpose=None)
        assert s.purpose is None

    def test_valid_default_none(self):
        """Default purpose is None."""
        s = ServerSettings()
        assert s.purpose is None

    @pytest.mark.parametrize(
        "value",
        ["a", "z", "0", "9", "abc", "a-b", "a1b", "my-app-v2", "x-y-z"],
    )
    def test_valid_parametrized(self, value: str):
        """Various valid purpose strings are accepted."""
        s = ServerSettings(purpose=value)
        assert s.purpose == value

    def test_invalid_uppercase(self):
        """Uppercase letters are rejected."""
        with pytest.raises(ValidationError, match="purpose must be lowercase"):
            ServerSettings(purpose="CreditScoring")

    def test_invalid_leading_hyphen(self):
        """Leading hyphen is rejected."""
        with pytest.raises(ValidationError, match="purpose must be lowercase"):
            ServerSettings(purpose="-credit")

    def test_invalid_trailing_hyphen(self):
        """Trailing hyphen is rejected."""
        with pytest.raises(ValidationError, match="purpose must be lowercase"):
            ServerSettings(purpose="credit-")

    def test_invalid_spaces(self):
        """Spaces are rejected."""
        with pytest.raises(ValidationError, match="purpose must be lowercase"):
            ServerSettings(purpose="credit scoring")

    def test_invalid_underscores(self):
        """Underscores are rejected."""
        with pytest.raises(ValidationError, match="purpose must be lowercase"):
            ServerSettings(purpose="credit_scoring")

    def test_invalid_special_chars(self):
        """Special characters are rejected."""
        with pytest.raises(ValidationError, match="purpose must be lowercase"):
            ServerSettings(purpose="credit@scoring")

    def test_invalid_empty_string(self):
        """Empty string is rejected."""
        with pytest.raises(ValidationError, match="purpose must be lowercase"):
            ServerSettings(purpose="")

    def test_invalid_only_hyphens(self):
        """Only hyphens is rejected."""
        with pytest.raises(ValidationError, match="purpose must be lowercase"):
            ServerSettings(purpose="---")

    @pytest.mark.parametrize(
        "value",
        ["", "-", "-a", "a-", "A", "a_b", "a b", "-a-b", "a-b-"],
    )
    def test_invalid_parametrized(self, value: str):
        """Various invalid purpose strings are rejected."""
        with pytest.raises(ValidationError, match="purpose must be lowercase"):
            ServerSettings(purpose=value)


class TestPurposeCLIArg:
    """Test --purpose CLI argument override in main()."""

    @pytest.mark.asyncio
    async def test_purpose_cli_overrides_default(self):
        """--purpose CLI arg sets settings.purpose."""
        from pg_airman_mcp.server import main

        with (
            patch("pg_airman_mcp.server.ServerSettings") as mock_settings,
            patch("pg_airman_mcp.server.create_mcp_server") as mock_create,
            patch("pg_airman_mcp.server.db_connection") as mock_db,
            patch(
                "sys.argv",
                ["server.py", "postgresql://test", "--purpose", "credit-scoring"],
            ),
            patch("signal.signal"),
        ):
            mock_settings_instance = MagicMock()
            mock_settings_instance.database_url = "postgresql://test"
            mock_settings_instance.access_mode = "unrestricted"
            mock_settings_instance.transport = "stdio"
            mock_settings_instance.auth_enabled = False
            mock_settings_instance.purpose = None
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

            assert mock_settings_instance.purpose == "credit-scoring"

    @pytest.mark.asyncio
    async def test_purpose_env_var(self, monkeypatch):
        """AIRMAN_MCP_PURPOSE env var sets purpose when no CLI arg."""
        monkeypatch.setenv("AIRMAN_MCP_PURPOSE", "my-purpose")
        s = ServerSettings()
        assert s.purpose == "my-purpose"


class TestPurposeConnectionURL:
    """Test connection URL application_name injection in main()."""

    @pytest.mark.asyncio
    async def test_appends_application_name_no_query_string(self):
        """Appends ?application_name=<purpose> when URL has no query string."""
        from pg_airman_mcp.server import main

        with (
            patch("pg_airman_mcp.server.ServerSettings") as mock_settings,
            patch("pg_airman_mcp.server.create_mcp_server") as mock_create,
            patch("pg_airman_mcp.server.db_connection") as mock_db,
            patch("sys.argv", ["server.py", "postgresql://localhost/testdb"]),
            patch("signal.signal"),
        ):
            mock_settings_instance = MagicMock()
            mock_settings_instance.database_url = "postgresql://localhost/testdb"
            mock_settings_instance.access_mode = "unrestricted"
            mock_settings_instance.transport = "stdio"
            mock_settings_instance.auth_enabled = False
            mock_settings_instance.purpose = "credit-scoring"
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

                with pytest.raises(SystemExit):
                    await main()

            # Verify pool_connect was called with the modified URL
            # Format: airman:<purpose> (URL-encoded colon = %3A)
            call_args = mock_db.pool_connect.call_args
            url = call_args[0][0]
            assert (
                url
                == "postgresql://localhost/testdb?application_name=airman%3Acredit-scoring"
            )

    @pytest.mark.asyncio
    async def test_appends_application_name_with_existing_query_string(self):
        """Appends &application_name=<purpose> when URL already has query params."""
        from pg_airman_mcp.server import main

        with (
            patch("pg_airman_mcp.server.ServerSettings") as mock_settings,
            patch("pg_airman_mcp.server.create_mcp_server") as mock_create,
            patch("pg_airman_mcp.server.db_connection") as mock_db,
            patch(
                "sys.argv",
                ["server.py", "postgresql://localhost/testdb?sslmode=require"],
            ),
            patch("signal.signal"),
        ):
            mock_settings_instance = MagicMock()
            mock_settings_instance.database_url = (
                "postgresql://localhost/testdb?sslmode=require"
            )
            mock_settings_instance.access_mode = "unrestricted"
            mock_settings_instance.transport = "stdio"
            mock_settings_instance.auth_enabled = False
            mock_settings_instance.purpose = "credit-scoring"
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

                with pytest.raises(SystemExit):
                    await main()

            # Format: airman:<purpose> (URL-encoded colon = %3A)
            url = mock_db.pool_connect.call_args[0][0]
            assert url == (
                "postgresql://localhost/testdb"
                "?sslmode=require&application_name=airman%3Acredit-scoring"
            )

    @pytest.mark.asyncio
    async def test_no_append_when_purpose_is_none(self):
        """No application_name appended when purpose is None."""
        from pg_airman_mcp.server import main

        with (
            patch("pg_airman_mcp.server.ServerSettings") as mock_settings,
            patch("pg_airman_mcp.server.create_mcp_server") as mock_create,
            patch("pg_airman_mcp.server.db_connection") as mock_db,
            patch("sys.argv", ["server.py", "postgresql://localhost/testdb"]),
            patch("signal.signal"),
        ):
            mock_settings_instance = MagicMock()
            mock_settings_instance.database_url = "postgresql://localhost/testdb"
            mock_settings_instance.access_mode = "unrestricted"
            mock_settings_instance.transport = "stdio"
            mock_settings_instance.auth_enabled = False
            mock_settings_instance.purpose = None
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

                with pytest.raises(SystemExit):
                    await main()

            url = mock_db.pool_connect.call_args[0][0]
            assert url == "postgresql://localhost/testdb"
            assert "application_name" not in url

    @pytest.mark.asyncio
    async def test_purpose_value_is_passed_through_quote(self):
        """Purpose value is passed through urllib.parse.quote in connection URL."""
        from pg_airman_mcp.server import main

        with (
            patch("pg_airman_mcp.server.ServerSettings") as mock_settings,
            patch("pg_airman_mcp.server.create_mcp_server") as mock_create,
            patch("pg_airman_mcp.server.db_connection") as mock_db,
            patch("pg_airman_mcp.server.quote", return_value="app-v2") as mock_quote,
            patch("sys.argv", ["server.py", "postgresql://localhost/testdb"]),
            patch("signal.signal"),
        ):
            mock_settings_instance = MagicMock()
            mock_settings_instance.database_url = "postgresql://localhost/testdb"
            mock_settings_instance.access_mode = "unrestricted"
            mock_settings_instance.transport = "stdio"
            mock_settings_instance.auth_enabled = False
            mock_settings_instance.purpose = "app-v2"
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

                with pytest.raises(SystemExit):
                    await main()

            # Verify quote() was called with the airman-prefixed purpose value
            mock_quote.assert_called_once_with("airman:app-v2")
            url = mock_db.pool_connect.call_args[0][0]
            assert "application_name=app-v2" in url

    @pytest.mark.asyncio
    async def test_purpose_logs_at_startup(self):
        """Purpose injection logs an info message."""
        from pg_airman_mcp.server import main

        with (
            patch("pg_airman_mcp.server.ServerSettings") as mock_settings,
            patch("pg_airman_mcp.server.create_mcp_server") as mock_create,
            patch("pg_airman_mcp.server.db_connection") as mock_db,
            patch("pg_airman_mcp.server.logger") as mock_logger,
            patch("sys.argv", ["server.py", "postgresql://localhost/testdb"]),
            patch("signal.signal"),
        ):
            mock_settings_instance = MagicMock()
            mock_settings_instance.database_url = "postgresql://localhost/testdb"
            mock_settings_instance.access_mode = "unrestricted"
            mock_settings_instance.transport = "stdio"
            mock_settings_instance.auth_enabled = False
            mock_settings_instance.purpose = "credit-scoring"
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

                with pytest.raises(SystemExit):
                    await main()

            # Check that purpose was logged
            log_messages = [str(call) for call in mock_logger.info.call_args_list]
            assert any(
                "credit-scoring" in msg and "application_name" in msg
                for msg in log_messages
            ), f"Expected purpose log message, got: {log_messages}"
