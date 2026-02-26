"""
Auth configuration and wiring for the MCP server.

This module provides the bridge between server configuration (ServerSettings)
and the generic IntrospectionTokenVerifier (RFC 7662). It owns:

- AuthContext: dataclass bundling token_verifier + AuthSettings for FastMCP
- create_auth_context(): factory that assembles auth objects from config
- add_auth_cli_args(): registers auth-related argparse arguments
- apply_auth_cli_overrides(): applies parsed CLI args onto ServerSettings

The module is intentionally decoupled from token_verifier.py (which is already
generic) so the auth wiring can eventually move into a separate package.
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from mcp.server.auth.settings import AuthSettings
from pydantic import AnyHttpUrl

from .token_verifier import IntrospectionTokenVerifier

if TYPE_CHECKING:
    from .server import ServerSettings

logger = logging.getLogger(__name__)


@dataclass
class AuthContext:
    """Result of auth wiring: objects needed by FastMCP.

    Attributes:
        token_verifier: Configured IntrospectionTokenVerifier for RFC 7662
            token validation.
        auth_settings: MCP SDK AuthSettings for the FastMCP instance.
    """

    token_verifier: IntrospectionTokenVerifier
    auth_settings: AuthSettings


def create_auth_context(
    auth_server_url: str,
    server_url: str,
    introspection_endpoint: str | None = None,
    required_scopes: list[str] | None = None,
    validate_resource: bool = False,
    introspection_client_id: str | None = None,
    introspection_client_secret: str | None = None,
) -> AuthContext:
    """
    Create the auth objects (token verifier + AuthSettings) for an
    authenticated FastMCP server.

    The introspection endpoint can point to any RFC 7662-compliant server,
    not just the built-in pg-airman-auth. When not provided, it defaults
    to ``{auth_server_url}/introspect``.

    Args:
        auth_server_url: Authorization server URL (used as OAuth issuer).
        server_url: This MCP server's URL for resource validation.
        introspection_endpoint: Token introspection endpoint URL.
            Defaults to ``{auth_server_url}/introspect`` if not provided.
        required_scopes: List of required OAuth scopes.
            Defaults to ``["mcp:postgres:access"]``.
        validate_resource: Enable strict RFC 8707 resource validation.
        introspection_client_id: OAuth client ID for introspection auth.
        introspection_client_secret: OAuth client secret for introspection auth.

    Returns:
        AuthContext with configured token_verifier and auth_settings.

    Raises:
        ValueError: If auth_server_url or server_url is missing.
    """
    if not auth_server_url:
        raise ValueError("--auth-server-url is required when --auth-enabled is set")

    if not server_url:
        raise ValueError(
            "Server URL is required when --auth-enabled is set. "
            "For SSE/HTTP transports, this is auto-detected. "
            "For stdio, set AIRMAN_MCP_SERVER_URL environment variable."
        )

    # Default introspection endpoint to {auth_server_url}/introspect
    if not introspection_endpoint:
        introspection_endpoint = f"{auth_server_url.rstrip('/')}/introspect"

    # Create token verifier
    token_verifier = IntrospectionTokenVerifier(
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

    scopes = required_scopes or ["mcp:postgres:access"]

    # Configure MCP SDK auth settings
    auth_settings = AuthSettings(
        issuer_url=AnyHttpUrl(auth_server_url),
        required_scopes=scopes,
        resource_server_url=AnyHttpUrl(server_url),
    )

    logger.info("=" * 60)
    logger.info("Authentication ENABLED")
    logger.info(f"  Issuer: {auth_server_url}")
    logger.info(f"  Introspection: {introspection_endpoint}")
    logger.info(f"  Server URL: {server_url}")
    logger.info(f"  Required scopes: {scopes}")
    logger.info(f"  Resource validation: {validate_resource}")
    logger.info("  All tools require valid OAuth access tokens")
    logger.info("=" * 60)

    return AuthContext(
        token_verifier=token_verifier,
        auth_settings=auth_settings,
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
    settings.auth_server_url = args.auth_server_url
    settings.auth_introspection_endpoint = args.auth_introspection_endpoint
    settings.auth_required_scopes = args.auth_required_scopes
    settings.auth_validate_resource = args.auth_validate_resource
    settings.auth_introspection_client_id = args.auth_introspection_client_id
    settings.auth_introspection_client_secret = args.auth_introspection_client_secret
