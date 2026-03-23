"""Unit tests for auth_config module."""

import argparse
from unittest.mock import MagicMock, patch

import pytest

from pg_airman_mcp.auth_config import (
    VALID_AUTH_TYPES,
    AuthContext,
    JwtConfig,
    add_auth_cli_args,
    apply_auth_cli_overrides,
    create_auth_context,
)

# Mock path for IntrospectionTokenVerifier (lazy import inside
# _create_introspection_verifier via ``from .token_verifier import ...``)
_INTROSPECTION_MOCK = "pg_airman_mcp.token_verifier.IntrospectionTokenVerifier"

# Mock path for JwtTokenVerifier (lazy import inside
# _create_jwt_verifier via ``from .jwt_verifier import ...``)
_JWT_MOCK = "pg_airman_mcp.jwt_verifier.JwtTokenVerifier"


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
        with patch(_INTROSPECTION_MOCK) as mock_verifier:
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
        with patch(_INTROSPECTION_MOCK) as mock_verifier:
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
        with patch(_INTROSPECTION_MOCK) as mock_verifier:
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
        with patch(_INTROSPECTION_MOCK) as mock_verifier:
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
            patch(_INTROSPECTION_MOCK) as mock_verifier,
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
            patch(_INTROSPECTION_MOCK) as mock_verifier,
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
        with patch(_INTROSPECTION_MOCK) as mock_verifier:
            mock_verifier.return_value = MagicMock()

            ctx = create_auth_context(
                auth_server_url="http://auth.example.com",
                server_url="http://resource.example.com",
            )

            assert ctx.auth_settings.required_scopes == ["mcp:postgres:access"]

    def test_custom_scopes(self):
        """Test custom scopes are passed through."""
        with patch(_INTROSPECTION_MOCK) as mock_verifier:
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
            patch(_INTROSPECTION_MOCK) as mock_verifier,
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

    def test_default_auth_type_is_introspection(self):
        """Test default auth_type creates an introspection verifier."""
        with patch(_INTROSPECTION_MOCK) as mock_verifier:
            mock_verifier.return_value = MagicMock()

            ctx = create_auth_context(
                auth_server_url="http://auth.example.com",
                server_url="http://resource.example.com",
            )

            # IntrospectionTokenVerifier should have been called, not JWT
            mock_verifier.assert_called_once()
            assert isinstance(ctx, AuthContext)

    def test_invalid_auth_type(self):
        """Test ValueError for an invalid auth_type."""
        with pytest.raises(ValueError, match="auth_type must be one of"):
            create_auth_context(
                auth_server_url="http://auth.example.com",
                server_url="http://resource.example.com",
                auth_type="invalid",  # type: ignore[arg-type]
            )

    def test_creates_jwt_verifier(self):
        """Test auth_type='jwt' creates a JwtTokenVerifier with correct params."""
        with patch(_JWT_MOCK) as mock_jwt:
            mock_jwt.return_value = MagicMock()

            ctx = create_auth_context(
                auth_server_url="http://auth.example.com",
                server_url="http://resource.example.com",
                auth_type="jwt",
                jwt_config=JwtConfig(
                    jwks_url="http://auth.example.com/jwks",
                    issuer="http://auth.example.com",
                    audience="http://resource.example.com",
                    algorithms="RS256,ES256",
                    clock_skew=60,
                    jwks_cache_ttl=600,
                ),
            )

            assert isinstance(ctx, AuthContext)
            mock_jwt.assert_called_once_with(
                jwks_url="http://auth.example.com/jwks",
                issuer="http://auth.example.com",
                audience="http://resource.example.com",
                algorithms=["RS256", "ES256"],
                clock_skew_seconds=60,
                jwks_cache_ttl=600,
            )

    def test_jwt_default_jwks_url(self):
        """Test JWKS URL defaults to {auth_server_url}/.well-known/jwks.json."""
        with patch(_JWT_MOCK) as mock_jwt:
            mock_jwt.return_value = MagicMock()

            create_auth_context(
                auth_server_url="http://auth.example.com",
                server_url="http://resource.example.com",
                auth_type="jwt",
                jwt_config=JwtConfig(issuer="http://auth.example.com"),
            )

            kwargs = mock_jwt.call_args.kwargs
            assert kwargs["jwks_url"] == "http://auth.example.com/.well-known/jwks.json"

    def test_jwt_default_audience(self):
        """Test JWT audience defaults to server_url when not provided."""
        with patch(_JWT_MOCK) as mock_jwt:
            mock_jwt.return_value = MagicMock()

            create_auth_context(
                auth_server_url="http://auth.example.com",
                server_url="http://resource.example.com",
                auth_type="jwt",
                jwt_config=JwtConfig(
                    issuer="http://auth.example.com",
                ),
            )

            kwargs = mock_jwt.call_args.kwargs
            assert kwargs["audience"] == "http://resource.example.com"

    def test_jwt_custom_jwks_url(self):
        """Test custom JWKS URL is passed through."""
        with patch(_JWT_MOCK) as mock_jwt:
            mock_jwt.return_value = MagicMock()

            create_auth_context(
                auth_server_url="http://auth.example.com",
                server_url="http://resource.example.com",
                auth_type="jwt",
                jwt_config=JwtConfig(
                    jwks_url="http://custom.example.com/keys",
                    issuer="http://auth.example.com",
                ),
            )

            kwargs = mock_jwt.call_args.kwargs
            assert kwargs["jwks_url"] == "http://custom.example.com/keys"

    def test_jwt_requires_issuer(self):
        """Test ValueError when auth_type='jwt' and issuer is None."""
        with pytest.raises(ValueError, match="--jwt-issuer is required"):
            create_auth_context(
                auth_server_url="http://auth.example.com",
                server_url="http://resource.example.com",
                auth_type="jwt",
                jwt_config=JwtConfig(issuer=None),
            )

    def test_jwt_parses_algorithms(self):
        """Test comma-separated algorithms are parsed into a list."""
        with patch(_JWT_MOCK) as mock_jwt:
            mock_jwt.return_value = MagicMock()

            create_auth_context(
                auth_server_url="http://auth.example.com",
                server_url="http://resource.example.com",
                auth_type="jwt",
                jwt_config=JwtConfig(
                    issuer="http://auth.example.com",
                    algorithms="RS256,ES256",
                ),
            )

            kwargs = mock_jwt.call_args.kwargs
            assert kwargs["algorithms"] == ["RS256", "ES256"]

    def test_jwt_logs_auth_info(self):
        """Test JWT-specific info is logged."""
        with (
            patch(_JWT_MOCK) as mock_jwt,
            patch("pg_airman_mcp.auth_config.logger") as mock_logger,
        ):
            mock_jwt.return_value = MagicMock()

            create_auth_context(
                auth_server_url="http://auth.example.com",
                server_url="http://resource.example.com",
                auth_type="jwt",
                jwt_config=JwtConfig(
                    jwks_url="http://auth.example.com/jwks",
                    issuer="http://auth.example.com",
                    audience="http://resource.example.com",
                    algorithms="RS256",
                ),
            )

            mock_logger.info.assert_any_call("Authentication ENABLED")
            mock_logger.info.assert_any_call("  Auth type: jwt")
            mock_logger.info.assert_any_call("  JWKS URL: http://auth.example.com/jwks")
            mock_logger.info.assert_any_call("  JWT issuer: http://auth.example.com")
            mock_logger.info.assert_any_call(
                "  JWT audience: http://resource.example.com"
            )
            mock_logger.info.assert_any_call("  JWT algorithms: RS256")

    def test_jwt_validate_resource_raises(self):
        """Test ValueError raised when validate_resource=True with JWT auth type."""
        with pytest.raises(ValueError, match="cannot be used with --auth-type=jwt"):
            create_auth_context(
                auth_server_url="http://auth.example.com",
                server_url="http://resource.example.com",
                auth_type="jwt",
                validate_resource=True,
                jwt_config=JwtConfig(issuer="http://auth.example.com"),
            )

    def test_jwt_validate_resource_false_succeeds(self):
        """Test JWT auth succeeds when validate_resource=False (default)."""
        with patch(_JWT_MOCK) as mock_jwt:
            mock_jwt.return_value = MagicMock()

            # Should not raise
            ctx = create_auth_context(
                auth_server_url="http://auth.example.com",
                server_url="http://resource.example.com",
                auth_type="jwt",
                validate_resource=False,
                jwt_config=JwtConfig(issuer="http://auth.example.com"),
            )
            assert ctx is not None

    def test_auth_type_case_insensitive(self):
        """Test auth_type accepts uppercase and normalizes to lowercase."""
        with patch(_JWT_MOCK) as mock_jwt:
            mock_jwt.return_value = MagicMock()

            ctx = create_auth_context(
                auth_server_url="http://auth.example.com",
                server_url="http://resource.example.com",
                auth_type="JWT",  # type: ignore[arg-type]
                jwt_config=JwtConfig(issuer="http://auth.example.com"),
            )

            assert isinstance(ctx, AuthContext)

    def test_jwt_no_introspection_warning(self):
        """Test no introspection credentials warning for JWT auth type."""
        with (
            patch(_JWT_MOCK) as mock_jwt,
            patch("pg_airman_mcp.auth_config.logger") as mock_logger,
        ):
            mock_jwt.return_value = MagicMock()

            create_auth_context(
                auth_server_url="http://auth.example.com",
                server_url="http://resource.example.com",
                auth_type="jwt",
                jwt_config=JwtConfig(
                    issuer="http://auth.example.com",
                ),
            )

            # Should not have warned about introspection credentials
            for call in mock_logger.warning.call_args_list:
                assert "Introspection client credentials" not in str(call)


class TestAddAuthCliArgs:
    """Test add_auth_cli_args registers expected CLI arguments."""

    def _make_parser_with_args(self) -> argparse.ArgumentParser:
        """Create a parser with auth CLI args registered."""
        from pg_airman_mcp.server import ServerSettings

        parser = argparse.ArgumentParser()
        settings = ServerSettings()
        add_auth_cli_args(parser, settings)
        return parser

    def test_registers_auth_type_arg(self):
        """Test --auth-type is registered with correct choices."""
        parser = self._make_parser_with_args()
        # Find the --auth-type action
        action = None
        for a in parser._actions:
            if "--auth-type" in getattr(a, "option_strings", []):
                action = a
                break
        assert action is not None, "--auth-type not registered"
        assert action.choices is not None
        assert set(action.choices) == set(VALID_AUTH_TYPES)

    def test_registers_jwt_args(self):
        """Test all --jwt-* arguments are registered."""
        parser = self._make_parser_with_args()
        registered = set()
        for a in parser._actions:
            for opt in getattr(a, "option_strings", []):
                registered.add(opt)

        expected_jwt_args = [
            "--jwt-jwks-url",
            "--jwt-issuer",
            "--jwt-audience",
            "--jwt-algorithms",
            "--jwt-clock-skew",
            "--jwt-jwks-cache-ttl",
        ]
        for arg in expected_jwt_args:
            assert arg in registered, f"{arg} not registered"


class TestApplyAuthCliOverrides:
    """Test apply_auth_cli_overrides applies parsed args to settings."""

    def test_applies_auth_type(self):
        """Test auth_type is applied from CLI args."""
        from pg_airman_mcp.server import ServerSettings

        settings = ServerSettings()
        args = argparse.Namespace(
            auth_enabled=True,
            auth_type="jwt",
            auth_server_url="http://auth.example.com",
            auth_introspection_endpoint=None,
            auth_required_scopes="mcp:postgres:access",
            auth_validate_resource=False,
            auth_introspection_client_id=None,
            auth_introspection_client_secret=None,
            jwt_jwks_url=None,
            jwt_issuer="http://auth.example.com",
            jwt_audience=None,
            jwt_algorithms="RS256",
            jwt_clock_skew=30,
            jwt_jwks_cache_ttl=300,
        )
        apply_auth_cli_overrides(settings, args)
        assert settings.auth_type == "jwt"

    def test_applies_jwt_settings(self):
        """Test all JWT settings are applied from CLI args."""
        from pg_airman_mcp.server import ServerSettings

        settings = ServerSettings()
        args = argparse.Namespace(
            auth_enabled=True,
            auth_type="jwt",
            auth_server_url="http://auth.example.com",
            auth_introspection_endpoint=None,
            auth_required_scopes="mcp:postgres:access",
            auth_validate_resource=False,
            auth_introspection_client_id=None,
            auth_introspection_client_secret=None,
            jwt_jwks_url="http://auth.example.com/jwks",
            jwt_issuer="http://auth.example.com",
            jwt_audience="http://resource.example.com",
            jwt_algorithms="RS256,ES256",
            jwt_clock_skew=60,
            jwt_jwks_cache_ttl=600,
        )
        apply_auth_cli_overrides(settings, args)

        assert settings.jwt_jwks_url == "http://auth.example.com/jwks"
        assert settings.jwt_issuer == "http://auth.example.com"
        assert settings.jwt_audience == "http://resource.example.com"
        assert settings.jwt_algorithms == "RS256,ES256"
        assert settings.jwt_clock_skew == 60
        assert settings.jwt_jwks_cache_ttl == 600
