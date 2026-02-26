"""
Token verifier for validating OAuth access tokens via introspection.

This module implements OAuth 2.0 Token Introspection (RFC 7662) to validate
access tokens issued by any OAuth authorization server. It's used by the
MCP server to protect tools and resources with authentication.
"""

import logging
from typing import Any
from urllib.parse import urlparse

import httpx
from mcp.server.auth.provider import AccessToken, TokenVerifier
from mcp.shared.auth_utils import check_resource_allowed, resource_url_from_server_url

logger = logging.getLogger(__name__)


class IntrospectionTokenVerifier(TokenVerifier):
    """
    Token verifier using OAuth 2.0 Token Introspection (RFC 7662).

    This verifier validates access tokens by calling the authorization server's
    introspection endpoint. It checks if tokens are active, optionally validates
    the resource parameter (RFC 8707), and returns token metadata.

    Token Validation Flow:
    1. MCP client makes request with "Authorization: Bearer {token}"
    2. MCP server extracts token and calls verify_token()
    3. Verifier sends token to introspection endpoint
    4. Authorization server validates token and returns metadata
    5. If active=true, request is allowed; if active=false, returns 401

    Security Features:
    - SSRF prevention (only allows https:// or localhost)
    - SSL verification enforced (verify=True)
    - Connection pooling and timeouts
    - RFC 8707 resource validation (optional)
    - Comprehensive error handling

    Args:
        introspection_endpoint: URL of the auth server's introspection endpoint
                               (e.g., "http://localhost:9000/introspect")
        server_url: URL of this MCP server for resource validation
                   (e.g., "http://localhost:8000")
        validate_resource: If True, enforces RFC 8707 resource parameter matching
        client_id: OAuth client ID for authenticating introspection requests
                  (RFC 7662 §2.1, client_secret_post method)
        client_secret: OAuth client secret for authenticating introspection requests

    Example:
        verifier = IntrospectionTokenVerifier(
            introspection_endpoint="http://localhost:9000/introspect",
            server_url="http://localhost:8000",
            validate_resource=True,
            client_id="my-service-client",
            client_secret="s3cr3t",
        )

        token = await verifier.verify_token("mcp_abc123...")
        if token:
            print(f"Valid token for client: {token.client_id}")
    """

    def __init__(
        self,
        introspection_endpoint: str,
        server_url: str,
        validate_resource: bool = False,
        client_id: str | None = None,
        client_secret: str | None = None,
    ):
        """
        Initialize the token verifier.

        Args:
            introspection_endpoint: Authorization server's introspection URL
            server_url: This server's URL for resource validation
            validate_resource: Enable strict RFC 8707 resource validation
            client_id: OAuth client ID for introspection authentication
            client_secret: OAuth client secret for introspection authentication
        """
        self.introspection_endpoint = introspection_endpoint
        self.server_url = server_url
        self.validate_resource = validate_resource
        self.resource_url = resource_url_from_server_url(server_url)
        self.client_id = client_id
        self.client_secret = client_secret
        self._client: httpx.AsyncClient | None = None

    def _get_client(self) -> httpx.AsyncClient:
        """Return the shared HTTP client, creating it on first use."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(10.0, connect=5.0),
                limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
                verify=True,
            )
        return self._client

    async def close(self) -> None:
        """Close the shared HTTP client.

        Should be called during server shutdown to release connections.
        """
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def verify_token(self, token: str) -> AccessToken | None:
        """
        Verify an access token via the introspection endpoint.

        This method calls the authorization server's introspection endpoint to
        validate the token. It checks:
        - Token is active (not expired or revoked)
        - Token was issued for this resource (if validate_resource=True)
        - Token has valid scopes and client_id

        Args:
            token: The access token string to verify (e.g., "mcp_abc123...")

        Returns:
            AccessToken object with token metadata if valid, None if invalid.
            Returns None for:
            - Inactive/expired/revoked tokens
            - Network/connection errors
            - Invalid introspection endpoint
            - Failed resource validation

        Security Notes:
            - Rejects non-HTTPS endpoints except localhost (SSRF prevention)
            - Enforces SSL certificate verification
            - Uses connection pooling and timeouts
            - Logs all validation failures for audit

        Example:
            token = await verifier.verify_token("mcp_abc123...")
            if token:
                # Token is valid, allow request
                print(f"Client: {token.client_id}, Scopes: {token.scopes}")
            else:
                # Token is invalid, return 401 Unauthorized
                return {"error": "unauthorized"}
        """
        # SSRF prevention: Validate introspection endpoint URL.
        # Parse the URL and check scheme + exact hostname to prevent
        # prefix tricks (e.g., http://localhost.evil.com).
        parsed = urlparse(self.introspection_endpoint)
        is_https = parsed.scheme == "https"
        is_local = parsed.scheme == "http" and parsed.hostname in (
            "localhost",
            "127.0.0.1",
            "::1",
        )
        if not (is_https or is_local):
            logger.warning(
                f"Rejecting introspection endpoint with unsafe URL: "
                f"{self.introspection_endpoint}"
            )
            return None

        client = self._get_client()
        try:
            # Build introspection request body (RFC 7662)
            form_data: dict[str, str] = {"token": token}
            if self.client_id and self.client_secret:
                form_data["client_id"] = self.client_id
                form_data["client_secret"] = self.client_secret

            # Call introspection endpoint per RFC 7662
            response = await client.post(
                self.introspection_endpoint,
                data=form_data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            # Check HTTP response status
            if response.status_code != 200:
                logger.debug(
                    f"Token introspection returned status {response.status_code}"
                )
                return None

            # Parse introspection response
            data = response.json()

            # Check if token is active (RFC 7662 requires "active" field)
            if not data.get("active", False):
                logger.debug("Token is inactive (expired or revoked)")
                return None

            # RFC 8707 resource validation (only when validate_resource=True)
            if self.validate_resource and not self._validate_resource(data):
                logger.warning(
                    f"Token resource validation failed. Expected: {self.resource_url}"
                )
                return None

            # Return validated token metadata
            return AccessToken(
                token=token,
                client_id=data.get("client_id", "unknown"),
                scopes=(data.get("scope", "").split() if data.get("scope") else []),
                expires_at=data.get("exp"),
                resource=data.get("aud"),  # RFC 8707 resource/audience
            )

        except httpx.TimeoutException:
            logger.warning(
                f"Token introspection timeout to {self.introspection_endpoint}"
            )
            return None
        except httpx.HTTPError as e:
            logger.warning(f"Token introspection HTTP error: {e}")
            return None
        except Exception as e:
            logger.warning(f"Token introspection failed: {e}")
            return None

    def _validate_resource(self, token_data: dict[str, Any]) -> bool:
        """
        Validate that the token was issued for this resource server.

        Implements RFC 8707 resource indicator validation by checking the
        token's "aud" (audience) claim against this server's resource URL.

        Why Resource Validation Matters:
        - Prevents token reuse across different services
        - Ensures tokens are only used for their intended resource
        - Provides defense-in-depth against token theft

        Args:
            token_data: Introspection response data containing token metadata

        Returns:
            True if token's resource/audience matches this server, False otherwise

        Example:
            # Token issued for "http://localhost:8000"
            # This server is "http://localhost:8000"
            # Result: True (exact match)

            # Token issued for "http://localhost:8000/api"
            # This server is "http://localhost:8000"
            # Result: True (hierarchical match)

            # Token issued for "http://other-server:8000"
            # This server is "http://localhost:8000"
            # Result: False (different resource)
        """
        if not self.server_url or not self.resource_url:
            # Fail if strict validation requested but URLs missing
            logger.warning("Resource validation enabled but URLs not configured")
            return False

        # Check 'aud' claim (standard JWT audience, RFC 8707)
        aud: list[str] | str | None = token_data.get("aud")

        if isinstance(aud, list):
            # Token may be issued for multiple resources (audience list)
            for audience in aud:
                if self._is_valid_resource(audience):
                    return True
            logger.debug(
                f"Token audiences {aud} do not match resource {self.resource_url}"
            )
            return False
        elif aud:
            # Single audience string
            return self._is_valid_resource(aud)

        # No resource binding - invalid per RFC 8707 when strict validation enabled
        logger.debug("Token has no audience (aud) claim")
        return False

    def _is_valid_resource(self, resource: str) -> bool:
        """
        Check if a resource matches this server using hierarchical matching.

        Uses the MCP SDK's check_resource_allowed utility for RFC 8707 compliant
        resource matching. Supports both exact and hierarchical matching.

        Hierarchical Matching:
        - Token for "http://localhost:8000" allows "http://localhost:8000/api"
        - Token for "http://localhost:8000/api" does NOT allow "http://localhost:8000"
        - Prevents privilege escalation via overly broad tokens

        Args:
            resource: Resource string from token's aud/resource claim

        Returns:
            True if resource is valid for this server, False otherwise
        """
        if not self.resource_url:
            return False

        # Use MCP SDK's hierarchical resource matching (RFC 8707)
        return check_resource_allowed(
            requested_resource=self.resource_url,
            configured_resource=resource,
        )
