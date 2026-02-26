"""Unit tests for ServerSettings and server configuration."""

import os
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from pg_airman_mcp.server import AccessMode, ServerSettings, create_mcp_server


class TestServerSettings:
    """Test ServerSettings configuration class."""

    def test_default_values(self):
        """Test default values are set correctly."""
        settings = ServerSettings()
        assert settings.database_url is None
        assert settings.access_mode == "unrestricted"
        assert settings.transport == "stdio"
        assert settings.sse_host == "localhost"
        assert settings.sse_port == 8000
        assert settings.streamable_http_host == "localhost"
        assert settings.streamable_http_port == 8001
        assert settings.auth_enabled is False
        assert settings.auth_server_url == "http://localhost:9000"
        assert settings.auth_introspection_endpoint is None
        assert settings.auth_required_scopes == "mcp:postgres:access"
        assert settings.auth_validate_resource is False
        assert settings.auth_introspection_client_id is None
        assert settings.auth_introspection_client_secret is None
        assert settings.server_url is None

    def test_custom_values(self):
        """Test custom values override defaults."""
        settings = ServerSettings(
            database_url="postgresql://user:pass@localhost/db",
            access_mode="restricted",
            transport="sse",
            sse_host="0.0.0.0",
            sse_port=9000,
            auth_enabled=True,
            auth_server_url="http://auth.example.com",
        )
        assert settings.database_url == "postgresql://user:pass@localhost/db"
        assert settings.access_mode == "restricted"
        assert settings.transport == "sse"
        assert settings.sse_host == "0.0.0.0"
        assert settings.sse_port == 9000
        assert settings.auth_enabled is True
        assert settings.auth_server_url == "http://auth.example.com"

    def test_env_prefix(self):
        """Test that environment variables use AIRMAN_MCP_ prefix."""
        with patch.dict(
            os.environ,
            {
                "AIRMAN_MCP_DATABASE_URL": "postgresql://env:pass@localhost/envdb",
                "AIRMAN_MCP_ACCESS_MODE": "restricted",
                "AIRMAN_MCP_TRANSPORT": "sse",
                "AIRMAN_MCP_AUTH_ENABLED": "true",
            },
        ):
            settings = ServerSettings()
            assert settings.database_url == "postgresql://env:pass@localhost/envdb"
            assert settings.access_mode == "restricted"
            assert settings.transport == "sse"
            assert settings.auth_enabled is True

    def test_access_mode_validator_valid(self):
        """Test access_mode validator accepts valid values."""
        # Lowercase
        settings = ServerSettings(access_mode="unrestricted")
        assert settings.access_mode == "unrestricted"

        settings = ServerSettings(access_mode="restricted")
        assert settings.access_mode == "restricted"

        # Uppercase should be converted to lowercase
        settings = ServerSettings(access_mode="UNRESTRICTED")
        assert settings.access_mode == "unrestricted"

        settings = ServerSettings(access_mode="RESTRICTED")
        assert settings.access_mode == "restricted"

    def test_access_mode_validator_invalid(self):
        """Test access_mode validator rejects invalid values."""
        with pytest.raises(ValidationError) as exc:
            ServerSettings(access_mode="invalid")
        assert "access_mode must be one of" in str(exc.value)

    def test_transport_validator_valid(self):
        """Test transport validator accepts valid values."""
        for transport in ["stdio", "sse", "streamable-http"]:
            settings = ServerSettings(transport=transport)
            assert settings.transport == transport

        # Uppercase should be converted to lowercase
        settings = ServerSettings(transport="STDIO")
        assert settings.transport == "stdio"

    def test_transport_validator_invalid(self):
        """Test transport validator rejects invalid values."""
        with pytest.raises(ValidationError) as exc:
            ServerSettings(transport="invalid")
        assert "transport must be one of" in str(exc.value)

    def test_get_required_scopes(self):
        """Test parsing required scopes from comma-separated string."""
        settings = ServerSettings(auth_required_scopes="read,write,admin")
        scopes = settings.get_required_scopes()
        assert scopes == ["read", "write", "admin"]

        # Test with spaces
        settings = ServerSettings(auth_required_scopes="read, write, admin")
        scopes = settings.get_required_scopes()
        assert scopes == ["read", "write", "admin"]

        # Test empty string
        settings = ServerSettings(auth_required_scopes="")
        scopes = settings.get_required_scopes()
        assert scopes == []

        # Test single scope
        settings = ServerSettings(auth_required_scopes="mcp:postgres:access")
        scopes = settings.get_required_scopes()
        assert scopes == ["mcp:postgres:access"]

    def test_determine_server_url_no_auth(self):
        """Test server URL determination when auth is disabled."""
        settings = ServerSettings(auth_enabled=False)
        assert settings.determine_server_url() is None

    def test_determine_server_url_explicit(self):
        """Test server URL determination with explicit server_url."""
        settings = ServerSettings(
            auth_enabled=True, server_url="http://explicit.example.com"
        )
        assert settings.determine_server_url() == "http://explicit.example.com"

    def test_determine_server_url_stdio(self):
        """Test server URL determination for stdio transport."""
        settings = ServerSettings(auth_enabled=True, transport="stdio")
        # stdio with auth but no explicit server_url returns None
        assert settings.determine_server_url() is None

    def test_determine_server_url_sse(self):
        """Test server URL determination for SSE transport."""
        settings = ServerSettings(
            auth_enabled=True, transport="sse", sse_host="0.0.0.0", sse_port=9000
        )
        # 0.0.0.0 is remapped to localhost for a usable URL
        assert settings.determine_server_url() == "http://localhost:9000"

    def test_determine_server_url_sse_specific_host(self):
        """Test server URL keeps specific host for SSE transport."""
        settings = ServerSettings(
            auth_enabled=True, transport="sse", sse_host="10.0.0.5", sse_port=9000
        )
        assert settings.determine_server_url() == "http://10.0.0.5:9000"

    def test_determine_server_url_streamable_http(self):
        """Test server URL determination for streamable-http transport."""
        settings = ServerSettings(
            auth_enabled=True,
            transport="streamable-http",
            streamable_http_host="0.0.0.0",
            streamable_http_port=9001,
        )
        # 0.0.0.0 is remapped to localhost for a usable URL
        assert settings.determine_server_url() == "http://localhost:9001"

    def test_determine_server_url_streamable_http_specific_host(self):
        """Test server URL keeps specific host for streamable-http transport."""
        settings = ServerSettings(
            auth_enabled=True,
            transport="streamable-http",
            streamable_http_host="192.168.1.10",
            streamable_http_port=9001,
        )
        assert settings.determine_server_url() == "http://192.168.1.10:9001"


class TestAccessMode:
    """Test AccessMode enum."""

    def test_enum_values(self):
        """Test AccessMode enum has expected values."""
        assert AccessMode.UNRESTRICTED.value == "unrestricted"
        assert AccessMode.RESTRICTED.value == "restricted"

    def test_enum_string_conversion(self):
        """Test converting strings to AccessMode enum."""
        assert AccessMode("unrestricted") == AccessMode.UNRESTRICTED
        assert AccessMode("restricted") == AccessMode.RESTRICTED


class TestCreateMcpServer:
    """Test create_mcp_server function."""

    def test_creates_unauthenticated_server(self):
        """Test creating server without authentication."""
        with patch("pg_airman_mcp.server.FastMCP") as mock_fastmcp:
            mock_instance = MagicMock()
            mock_fastmcp.return_value = mock_instance

            server = create_mcp_server(auth_context=None)

            # Should create FastMCP without auth parameters
            mock_fastmcp.assert_called_once_with("pg-airman-mcp")
            assert server == mock_instance

            # Should register 9 tools (execute_sql added separately in main)
            assert mock_instance.add_tool.call_count == 9

    def test_creates_authenticated_server(self):
        """Test creating server with AuthContext."""
        from pg_airman_mcp.auth_config import AuthContext

        with patch("pg_airman_mcp.server.FastMCP") as mock_fastmcp:
            mock_instance = MagicMock()
            mock_fastmcp.return_value = mock_instance

            # Create a mock AuthContext
            mock_verifier = MagicMock()
            mock_auth_settings = MagicMock()
            auth_ctx = AuthContext(
                token_verifier=mock_verifier,
                auth_settings=mock_auth_settings,
            )

            create_mcp_server(auth_context=auth_ctx)

            # Should create FastMCP with auth
            assert mock_fastmcp.call_count == 1
            call_kwargs = mock_fastmcp.call_args.kwargs
            assert call_kwargs["token_verifier"] is mock_verifier
            assert call_kwargs["auth"] is mock_auth_settings

            # Should register 9 tools
            assert mock_instance.add_tool.call_count == 9

    def test_tool_registration_list(self):
        """Test that all expected tools are registered."""
        with patch("pg_airman_mcp.server.FastMCP") as mock_fastmcp:
            mock_instance = MagicMock()
            mock_fastmcp.return_value = mock_instance

            create_mcp_server(auth_context=None)

            # Get all registered tool names
            registered_tools = []
            for call in mock_instance.add_tool.call_args_list:
                # First positional argument is the function
                tool_func = call[0][0]
                registered_tools.append(tool_func.__name__)

            # Check expected tools are registered (execute_sql excluded)
            expected_tools = [
                "list_schemas",
                "list_objects",
                "get_object_details",
                "explain_query",
                "analyze_workload_indexes",
                "analyze_query_indexes",
                "analyze_db_health",
                "get_top_queries",
                "add_comment_to_object",
            ]

            for tool in expected_tools:
                assert tool in registered_tools, f"Tool {tool} not registered"


class TestServerSettingsEnvironmentIntegration:
    """Test ServerSettings environment variable integration."""

    def test_all_env_vars_use_airman_mcp_prefix(self):
        """Test all environment variables use AIRMAN_MCP_ prefix."""
        env_vars = {
            "AIRMAN_MCP_DATABASE_URL": "postgresql://test:pass@host/db",
            "AIRMAN_MCP_ACCESS_MODE": "restricted",
            "AIRMAN_MCP_TRANSPORT": "sse",
            "AIRMAN_MCP_SSE_HOST": "custom.host",
            "AIRMAN_MCP_SSE_PORT": "9000",
            "AIRMAN_MCP_STREAMABLE_HTTP_HOST": "stream.host",
            "AIRMAN_MCP_STREAMABLE_HTTP_PORT": "9001",
            "AIRMAN_MCP_AUTH_ENABLED": "true",
            "AIRMAN_MCP_AUTH_SERVER_URL": "http://auth.test",
            "AIRMAN_MCP_AUTH_INTROSPECTION_ENDPOINT": "http://auth.test/check",
            "AIRMAN_MCP_AUTH_REQUIRED_SCOPES": "read,write",
            "AIRMAN_MCP_AUTH_VALIDATE_RESOURCE": "true",
            "AIRMAN_MCP_AUTH_INTROSPECTION_CLIENT_ID": "my-client",
            "AIRMAN_MCP_AUTH_INTROSPECTION_CLIENT_SECRET": "my-secret",
            "AIRMAN_MCP_SERVER_URL": "http://resource.test",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            settings = ServerSettings()

            # Verify all values loaded from environment
            assert settings.database_url == "postgresql://test:pass@host/db"
            assert settings.access_mode == "restricted"
            assert settings.transport == "sse"
            assert settings.sse_host == "custom.host"
            assert settings.sse_port == 9000
            assert settings.streamable_http_host == "stream.host"
            assert settings.streamable_http_port == 9001
            assert settings.auth_enabled is True
            assert settings.auth_server_url == "http://auth.test"
            assert settings.auth_introspection_endpoint == "http://auth.test/check"
            assert settings.auth_required_scopes == "read,write"
            assert settings.auth_validate_resource is True
            assert settings.auth_introspection_client_id == "my-client"
            assert settings.auth_introspection_client_secret == "my-secret"
            assert settings.server_url == "http://resource.test"

    def test_old_env_vars_not_recognized(self):
        """Test that old environment variable names are not recognized."""
        with patch.dict(
            os.environ,
            {
                "DATABASE_URI": "postgresql://old:pass@host/db",
                "MCP_DATABASE_URL": "postgresql://wrong:pass@host/db",
                "PG_AIRMAN_ACCESS_MODE": "restricted",
            },
            clear=True,
        ):
            settings = ServerSettings()

            # Should use defaults, not old env var names
            assert settings.database_url is None
            assert settings.access_mode == "unrestricted"

    def test_case_insensitive_env_vars(self):
        """Test that environment variables are case-insensitive."""
        with patch.dict(
            os.environ,
            {
                "airman_mcp_access_mode": "restricted",
                "AIRMAN_MCP_TRANSPORT": "sse",
            },
        ):
            settings = ServerSettings()
            assert settings.access_mode == "restricted"
            assert settings.transport == "sse"
