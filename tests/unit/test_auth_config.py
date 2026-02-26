"""Unit tests for auth_config module."""

from unittest.mock import MagicMock, patch

import pytest

from pg_airman_mcp.auth_config import AuthContext, create_auth_context


class TestAuthContext:
    """Test AuthContext dataclass."""

    def test_holds_verifier_and_settings(self):
        """Test AuthContext stores both fields."""
        mock_verifier = MagicMock()
        mock_settings = MagicMock()
        ctx = AuthContext(
            token_verifier=mock_verifier,
            auth_settings=mock_settings,
        )
        assert ctx.token_verifier is mock_verifier
        assert ctx.auth_settings is mock_settings


class TestCreateAuthContext:
    """Test create_auth_context factory function."""

    def test_creates_verifier_and_settings(self):
        """Test successful creation with all parameters."""
        with patch(
            "pg_airman_mcp.auth_config.IntrospectionTokenVerifier"
        ) as mock_verifier:
            mock_verifier.return_value = MagicMock()

            ctx = create_auth_context(
                auth_server_url="http://auth.example.com",
                server_url="http://resource.example.com",
                required_scopes=["read", "write"],
                validate_resource=True,
                introspection_client_id="test-client",
                introspection_client_secret="test-secret",
            )

            assert isinstance(ctx, AuthContext)
            mock_verifier.assert_called_once()
            kwargs = mock_verifier.call_args.kwargs
            assert kwargs["client_id"] == "test-client"
            assert kwargs["client_secret"] == "test-secret"
            assert kwargs["validate_resource"] is True

    def test_default_introspection_endpoint(self):
        """Test introspection endpoint defaults to {auth_server_url}/introspect."""
        with patch(
            "pg_airman_mcp.auth_config.IntrospectionTokenVerifier"
        ) as mock_verifier:
            mock_verifier.return_value = MagicMock()

            create_auth_context(
                auth_server_url="http://auth.example.com",
                server_url="http://resource.example.com",
                introspection_endpoint=None,
            )

            kwargs = mock_verifier.call_args.kwargs
            assert (
                kwargs["introspection_endpoint"] == "http://auth.example.com/introspect"
            )

    def test_default_introspection_endpoint_strips_trailing_slash(self):
        """Test trailing slash is stripped from auth_server_url."""
        with patch(
            "pg_airman_mcp.auth_config.IntrospectionTokenVerifier"
        ) as mock_verifier:
            mock_verifier.return_value = MagicMock()

            create_auth_context(
                auth_server_url="http://auth.example.com/",
                server_url="http://resource.example.com",
                introspection_endpoint=None,
            )

            kwargs = mock_verifier.call_args.kwargs
            assert (
                kwargs["introspection_endpoint"] == "http://auth.example.com/introspect"
            )

    def test_custom_introspection_endpoint(self):
        """Test using a custom introspection endpoint."""
        with patch(
            "pg_airman_mcp.auth_config.IntrospectionTokenVerifier"
        ) as mock_verifier:
            mock_verifier.return_value = MagicMock()

            create_auth_context(
                auth_server_url="http://auth.example.com",
                server_url="http://resource.example.com",
                introspection_endpoint="http://custom.example.com/check",
            )

            kwargs = mock_verifier.call_args.kwargs
            assert kwargs["introspection_endpoint"] == "http://custom.example.com/check"

    def test_requires_auth_server_url(self):
        """Test ValueError when auth_server_url is empty."""
        with pytest.raises(ValueError, match="--auth-server-url is required"):
            create_auth_context(
                auth_server_url="",
                server_url="http://resource.example.com",
            )

    def test_requires_server_url(self):
        """Test ValueError when server_url is empty."""
        with pytest.raises(ValueError, match="Server URL is required"):
            create_auth_context(
                auth_server_url="http://auth.example.com",
                server_url="",
            )

    def test_warns_when_credentials_missing(self):
        """Test warning logged when introspection credentials not configured."""
        with (
            patch(
                "pg_airman_mcp.auth_config.IntrospectionTokenVerifier"
            ) as mock_verifier,
            patch("pg_airman_mcp.auth_config.logger") as mock_logger,
        ):
            mock_verifier.return_value = MagicMock()

            create_auth_context(
                auth_server_url="http://auth.example.com",
                server_url="http://resource.example.com",
            )

            mock_logger.warning.assert_any_call(
                "Introspection client credentials not configured. "
                "Set AIRMAN_MCP_AUTH_INTROSPECTION_CLIENT_ID and "
                "AIRMAN_MCP_AUTH_INTROSPECTION_CLIENT_SECRET for "
                "authenticated introspection (RFC 7662 §2.1)."
            )

    def test_no_warning_when_credentials_provided(self):
        """Test no warning when both credentials are provided."""
        with (
            patch(
                "pg_airman_mcp.auth_config.IntrospectionTokenVerifier"
            ) as mock_verifier,
            patch("pg_airman_mcp.auth_config.logger") as mock_logger,
        ):
            mock_verifier.return_value = MagicMock()

            create_auth_context(
                auth_server_url="http://auth.example.com",
                server_url="http://resource.example.com",
                introspection_client_id="client-id",
                introspection_client_secret="client-secret",
            )

            # Should not have called warning about credentials
            for call in mock_logger.warning.call_args_list:
                assert "Introspection client credentials" not in str(call)

    def test_default_scopes(self):
        """Test default scopes when none provided."""
        with patch(
            "pg_airman_mcp.auth_config.IntrospectionTokenVerifier"
        ) as mock_verifier:
            mock_verifier.return_value = MagicMock()

            ctx = create_auth_context(
                auth_server_url="http://auth.example.com",
                server_url="http://resource.example.com",
            )

            assert ctx.auth_settings.required_scopes == ["mcp:postgres:access"]

    def test_custom_scopes(self):
        """Test custom scopes are passed through."""
        with patch(
            "pg_airman_mcp.auth_config.IntrospectionTokenVerifier"
        ) as mock_verifier:
            mock_verifier.return_value = MagicMock()

            ctx = create_auth_context(
                auth_server_url="http://auth.example.com",
                server_url="http://resource.example.com",
                required_scopes=["read", "write"],
            )

            assert ctx.auth_settings.required_scopes == ["read", "write"]

    def test_logs_auth_enabled_info(self):
        """Test that auth enabled info is logged."""
        with (
            patch(
                "pg_airman_mcp.auth_config.IntrospectionTokenVerifier"
            ) as mock_verifier,
            patch("pg_airman_mcp.auth_config.logger") as mock_logger,
        ):
            mock_verifier.return_value = MagicMock()

            create_auth_context(
                auth_server_url="http://auth.example.com",
                server_url="http://resource.example.com",
                introspection_client_id="cid",
                introspection_client_secret="csecret",
            )

            mock_logger.info.assert_any_call("Authentication ENABLED")
            mock_logger.info.assert_any_call("  Issuer: http://auth.example.com")
