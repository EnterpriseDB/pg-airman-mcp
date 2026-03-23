"""
Auth configuration and wiring for the MCP server.

This module provides the bridge between server configuration (ServerSettings)
and the token verifiers (IntrospectionTokenVerifier for RFC 7662,
JwtTokenVerifier for local JWT validation). It owns:

- AuthContext: dataclass bundling token_verifier + AuthSettings for FastMCP
- create_auth_context(): factory that assembles auth objects from config
- add_auth_cli_args(): registers auth-related argparse arguments
- apply_auth_cli_overrides(): applies parsed CLI args onto ServerSettings

The module is intentionally decoupled from token_verifier.py and
jwt_verifier.py so the auth wiring can eventually move into a separate package.
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from mcp.server.auth.provider import TokenVerifier
from mcp.server.auth.settings import AuthSettings
from pydantic import AnyHttpUrl

if TYPE_CHECKING:
    from .server import ServerSettings

logger = logging.getLogger(__name__)

VALID_AUTH_TYPES = ("introspection", "jwt")


@dataclass
class JwtConfig:
    """Configuration for JWT token verification.

    Attributes:
        jwks_url: JWKS endpoint URL. Defaults to
            ``{auth_server_url}/.well-known/jwks.json``.
        issuer: Expected ``iss`` claim. Required when auth_type is jwt.
        audience: Expected ``aud`` claim. Defaults to ``server_url``.
        algorithms: Comma-separated signing algorithms.
        clock_skew: Clock skew tolerance in seconds.
        jwks_cache_ttl: JWKS cache TTL in seconds.
    """

    jwks_url: str | None = None
    issuer: str | None = None
    audience: str | None = None
    algorithms: str = "RS256"
    clock_skew: int = 30
    jwks_cache_ttl: int = 300


@dataclass
class AuthContext:
    """Result of auth wiring: objects needed by FastMCP.

    Attributes:
        token_verifier: Configured token verifier implementing the
            TokenVerifier protocol (either IntrospectionTokenVerifier
            or JwtTokenVerifier).
        auth_settings: MCP SDK AuthSettings for the FastMCP instance.
    """

    token_verifier: TokenVerifier
    auth_settings: AuthSettings


def create_auth_context(
    auth_server_url: str,
    server_url: str,
    auth_type: Literal["introspection", "jwt"] = "introspection",
    introspection_endpoint: str | None = None,
    required_scopes: list[str] | None = None,
    validate_resource: bool = False,
    introspection_client_id: str | None = None,
    introspection_client_secret: str | None = None,
    jwt_config: JwtConfig | None = None,
) -> AuthContext:
    """
    Create the auth objects (token verifier + AuthSettings) for an
    authenticated FastMCP server.

    The ``auth_type`` parameter selects which verifier to create:

    - ``"introspection"`` (default): RFC 7662 token introspection via
      ``IntrospectionTokenVerifier``. Requires a reachable introspection
      endpoint.
    - ``"jwt"``: Local JWT validation via ``JwtTokenVerifier``. Requires
      a JWKS endpoint and issuer claim via ``jwt_config``.

    Args:
        auth_server_url: Authorization server URL (used as OAuth issuer).
        server_url: This MCP server's URL for resource validation.
        auth_type: Verifier type — ``"introspection"`` or ``"jwt"``.
        introspection_endpoint: Token introspection endpoint URL.
            Defaults to ``{auth_server_url}/introspect`` if not provided.
        required_scopes: List of required OAuth scopes.
            Defaults to ``["mcp:postgres:access"]``.
        validate_resource: Enable strict RFC 8707 resource validation.
        introspection_client_id: OAuth client ID for introspection auth.
        introspection_client_secret: OAuth client secret for
            introspection auth.
        jwt_config: JWT verification configuration. Required when
            auth_type is ``"jwt"``.

    Returns:
        AuthContext with configured token_verifier and auth_settings.

    Raises:
        ValueError: If required parameters are missing or auth_type
            is invalid.
    """
    auth_type = auth_type.lower()  # type: ignore[assignment]
    if auth_type not in VALID_AUTH_TYPES:
        raise ValueError(
            f"auth_type must be one of {VALID_AUTH_TYPES}, got: {auth_type!r}"
        )

    if not auth_server_url:
        raise ValueError("--auth-server-url is required when --auth-enabled is set")

    if not server_url:
        raise ValueError(
            "Server URL is required when --auth-enabled is set. "
            "For SSE/HTTP transports, this is auto-detected. "
            "For stdio, set AIRMAN_MCP_SERVER_URL environment variable."
        )

    jwt = jwt_config or JwtConfig()

    # Create the appropriate token verifier
    if auth_type == "introspection":
        token_verifier = _create_introspection_verifier(
            auth_server_url=auth_server_url,
            server_url=server_url,
            introspection_endpoint=introspection_endpoint,
            validate_resource=validate_resource,
            introspection_client_id=introspection_client_id,
            introspection_client_secret=introspection_client_secret,
        )
    else:
        if validate_resource:
            raise ValueError(
                "--auth-validate-resource cannot be used with --auth-type=jwt. "
                "JWT uses audience validation instead (--jwt-audience)."
            )
        token_verifier = _create_jwt_verifier(
            auth_server_url=auth_server_url,
            server_url=server_url,
            jwt_config=jwt,
        )

    scopes = required_scopes or ["mcp:postgres:access"]

    # Configure MCP SDK auth settings
    auth_settings = AuthSettings(
        issuer_url=AnyHttpUrl(auth_server_url),
        required_scopes=scopes,
        resource_server_url=AnyHttpUrl(server_url),
    )

    logger.info("=" * 60)
    logger.info("Authentication ENABLED")
    logger.info(f"  Auth type: {auth_type}")
    logger.info(f"  Issuer: {auth_server_url}")
    if auth_type == "introspection":
        endpoint = introspection_endpoint or f"{auth_server_url.rstrip('/')}/introspect"
        logger.info(f"  Introspection: {endpoint}")
    else:
        jwks = jwt.jwks_url or f"{auth_server_url.rstrip('/')}/.well-known/jwks.json"
        logger.info(f"  JWKS URL: {jwks}")
        logger.info(f"  JWT issuer: {jwt.issuer}")
        logger.info(f"  JWT audience: {jwt.audience or server_url}")
        logger.info(f"  JWT algorithms: {jwt.algorithms}")
    logger.info(f"  Server URL: {server_url}")
    logger.info(f"  Required scopes: {scopes}")
    logger.info(f"  Resource validation: {validate_resource}")
    logger.info("  All tools require valid OAuth access tokens")
    logger.info("=" * 60)

    return AuthContext(
        token_verifier=token_verifier,
        auth_settings=auth_settings,
    )


def _create_introspection_verifier(
    auth_server_url: str,
    server_url: str,
    introspection_endpoint: str | None,
    validate_resource: bool,
    introspection_client_id: str | None,
    introspection_client_secret: str | None,
) -> TokenVerifier:
    """Create an IntrospectionTokenVerifier for RFC 7662 token validation."""
    from .token_verifier import IntrospectionTokenVerifier

    # Default introspection endpoint to {auth_server_url}/introspect
    if not introspection_endpoint:
        introspection_endpoint = f"{auth_server_url.rstrip('/')}/introspect"

    verifier = IntrospectionTokenVerifier(
        introspection_endpoint=introspection_endpoint,
        server_url=server_url,
        validate_resource=validate_resource,
        client_id=introspection_client_id,
        client_secret=introspection_client_secret,
    )

    if not introspection_client_id or not introspection_client_secret:
        logger.warning(
            "Introspection client credentials not configured. "
            "Set AIRMAN_MCP_AUTH_INTROSPECTION_CLIENT_ID and "
            "AIRMAN_MCP_AUTH_INTROSPECTION_CLIENT_SECRET for "
            "authenticated introspection (RFC 7662 §2.1)."
        )

    return verifier


def _create_jwt_verifier(
    auth_server_url: str,
    server_url: str,
    jwt_config: JwtConfig,
) -> TokenVerifier:
    """Create a JwtTokenVerifier for local JWT validation."""
    from .jwt_verifier import JwtTokenVerifier

    if not jwt_config.issuer:
        raise ValueError(
            "--jwt-issuer is required when --auth-type=jwt. "
            "Set AIRMAN_MCP_JWT_ISSUER to the expected token issuer."
        )

    # Default JWKS URL to {auth_server_url}/.well-known/jwks.json
    jwks_url = (
        jwt_config.jwks_url or f"{auth_server_url.rstrip('/')}/.well-known/jwks.json"
    )

    # Default audience to server_url
    audience = jwt_config.audience or server_url

    # Parse algorithms from comma-separated string
    algorithms = [a.strip() for a in jwt_config.algorithms.split(",") if a.strip()]

    return JwtTokenVerifier(
        jwks_url=jwks_url,
        issuer=jwt_config.issuer,
        audience=audience,
        algorithms=algorithms,
        clock_skew_seconds=jwt_config.clock_skew,
        jwks_cache_ttl=jwt_config.jwks_cache_ttl,
    )


def add_auth_cli_args(
    parser: argparse.ArgumentParser,
    settings: ServerSettings,
) -> None:
    """
    Register auth-related CLI arguments on the given parser.

    Args:
        parser: The argparse.ArgumentParser to add arguments to.
        settings: ServerSettings instance providing defaults.
    """
    parser.add_argument(
        "--auth-enabled",
        action=argparse.BooleanOptionalAction,
        default=settings.auth_enabled,
        help="Enable OAuth authentication for all tools",
    )
    parser.add_argument(
        "--auth-type",
        type=str,
        choices=VALID_AUTH_TYPES,
        default=settings.auth_type,
        help=f"Authentication type (default: {settings.auth_type})",
    )
    parser.add_argument(
        "--auth-server-url",
        type=str,
        default=settings.auth_server_url,
        help=f"Authorization server URL (default: {settings.auth_server_url})",
    )
    parser.add_argument(
        "--auth-introspection-endpoint",
        type=str,
        default=settings.auth_introspection_endpoint,
        help="Token introspection endpoint URL (default: {auth-server-url}/introspect)",
    )
    parser.add_argument(
        "--auth-required-scopes",
        type=str,
        default=settings.auth_required_scopes,
        help=f"Comma-separated OAuth scopes (default: {settings.auth_required_scopes})",
    )
    parser.add_argument(
        "--auth-validate-resource",
        action=argparse.BooleanOptionalAction,
        default=settings.auth_validate_resource,
        help="Enable RFC 8707 resource validation",
    )
    parser.add_argument(
        "--auth-introspection-client-id",
        type=str,
        default=settings.auth_introspection_client_id,
        help="Client ID for authenticating introspection requests",
    )
    parser.add_argument(
        "--auth-introspection-client-secret",
        type=str,
        default=settings.auth_introspection_client_secret,
        help="Client secret for authenticating introspection requests",
    )

    # JWT-specific arguments
    parser.add_argument(
        "--jwt-jwks-url",
        type=str,
        default=settings.jwt_jwks_url,
        help="JWKS endpoint URL (default: {auth-server-url}/.well-known/jwks.json)",
    )
    parser.add_argument(
        "--jwt-issuer",
        type=str,
        default=settings.jwt_issuer,
        help="Expected JWT issuer claim (required when --auth-type=jwt)",
    )
    parser.add_argument(
        "--jwt-audience",
        type=str,
        default=settings.jwt_audience,
        help="Expected JWT audience claim (default: server URL)",
    )
    parser.add_argument(
        "--jwt-algorithms",
        type=str,
        default=settings.jwt_algorithms,
        help=f"Comma-separated JWT signing algorithms "
        f"(default: {settings.jwt_algorithms})",
    )
    parser.add_argument(
        "--jwt-clock-skew",
        type=int,
        default=settings.jwt_clock_skew,
        help=f"JWT clock skew tolerance in seconds "
        f"(default: {settings.jwt_clock_skew})",
    )
    parser.add_argument(
        "--jwt-jwks-cache-ttl",
        type=int,
        default=settings.jwt_jwks_cache_ttl,
        help=f"JWKS cache TTL in seconds (default: {settings.jwt_jwks_cache_ttl})",
    )


def apply_auth_cli_overrides(
    settings: ServerSettings,
    args: argparse.Namespace,
) -> None:
    """
    Apply parsed CLI auth arguments onto a ServerSettings instance.

    Args:
        settings: ServerSettings instance to update.
        args: Parsed argparse Namespace with auth fields.
    """
    settings.auth_enabled = args.auth_enabled
    settings.auth_type = args.auth_type
    settings.auth_server_url = args.auth_server_url
    settings.auth_introspection_endpoint = args.auth_introspection_endpoint
    settings.auth_required_scopes = args.auth_required_scopes
    settings.auth_validate_resource = args.auth_validate_resource
    settings.auth_introspection_client_id = args.auth_introspection_client_id
    settings.auth_introspection_client_secret = args.auth_introspection_client_secret
    settings.jwt_jwks_url = args.jwt_jwks_url
    settings.jwt_issuer = args.jwt_issuer
    settings.jwt_audience = args.jwt_audience
    settings.jwt_algorithms = args.jwt_algorithms
    settings.jwt_clock_skew = args.jwt_clock_skew
    settings.jwt_jwks_cache_ttl = args.jwt_jwks_cache_ttl
