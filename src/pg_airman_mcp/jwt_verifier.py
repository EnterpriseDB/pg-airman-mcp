"""
Token verifier for validating JWT access tokens using JWKS public keys.

This module implements local JWT (RFC 7519) token verification for the MCP
server. It fetches the identity provider's signing keys from a JWKS endpoint
(RFC 7517) and verifies token signatures locally — no per-request network
call to the authorization server is needed.
"""

import asyncio
import logging
import time
from datetime import timedelta
from typing import Any
from urllib.parse import urlparse

import httpx
import jwt
from mcp.server.auth.provider import AccessToken, TokenVerifier

logger = logging.getLogger(__name__)

# Algorithms that are never allowed regardless of configuration.
_FORBIDDEN_ALGORITHMS: frozenset[str] = frozenset({"none", "HS256", "HS384", "HS512"})

# Algorithms that can be configured for use.
ALLOWED_ALGORITHMS: frozenset[str] = frozenset(
    {"RS256", "RS384", "RS512", "ES256", "ES384", "ES512"}
)

# Minimum interval between consecutive JWKS endpoint refreshes (seconds).
_JWKS_MIN_REFRESH_INTERVAL: float = 30.0


def _is_safe_url(url: str) -> bool:
    """Check whether a URL passes the SSRF prevention rules.

    HTTPS is always allowed. HTTP is only permitted for exact localhost,
    127.0.0.1, or ::1 hostnames.

    Args:
        url: The URL to validate.

    Returns:
        True if the URL is considered safe, False otherwise.
    """
    parsed = urlparse(url)
    if parsed.scheme == "https":
        return True
    if parsed.scheme == "http" and parsed.hostname in (
        "localhost",
        "127.0.0.1",
        "::1",
    ):
        return True
    return False


class JwtTokenVerifier(TokenVerifier):
    """Token verifier using local JWT validation with JWKS public keys.

    This verifier validates access tokens by:

    1. Fetching signing keys from the identity provider's JWKS endpoint
    2. Verifying the JWT signature locally using the matching public key
    3. Validating standard claims (exp, iss, aud)
    4. Extracting client_id and scopes from JWT claims

    Security Features:
        - Only asymmetric algorithms accepted (RS256, ES256, etc.)
        - Symmetric algorithms (HS*) and ``none`` are explicitly rejected
        - SSRF prevention on JWKS URL (HTTPS required, HTTP only for localhost)
        - JWKS cache with configurable TTL and rate-limited refresh
        - Fail-closed: any verification error returns None (access denied)

    Args:
        jwks_url: URL of the identity provider's JWKS endpoint.
        issuer: Expected ``iss`` claim value.
        audience: Expected ``aud`` claim value.
        algorithms: Allowed signing algorithms. Defaults to ``["RS256"]``.
        clock_skew_seconds: Tolerance for clock differences when
            validating ``exp``. Defaults to 30.
        jwks_cache_ttl: Seconds before the JWKS cache is considered stale.
            Defaults to 300 (5 minutes).

    Raises:
        ValueError: If any algorithm is not in the allowed set, or if the
            JWKS URL fails SSRF validation.

    Example:
        verifier = JwtTokenVerifier(
            jwks_url="https://auth.example.com/.well-known/jwks.json",
            issuer="https://auth.example.com",
            audience="https://mcp.example.com",
        )

        token = await verifier.verify_token("eyJhbGciOi...")
        if token:
            print(f"Valid token for client: {token.client_id}")
    """

    def __init__(
        self,
        jwks_url: str,
        issuer: str,
        audience: str,
        algorithms: list[str] | None = None,
        clock_skew_seconds: int = 30,
        jwks_cache_ttl: int = 300,
    ) -> None:
        algorithms = algorithms or ["RS256"]

        # Validate algorithms at construction time.
        for alg in algorithms:
            if alg in _FORBIDDEN_ALGORITHMS:
                raise ValueError(
                    f"Algorithm '{alg}' is forbidden. "
                    "Only asymmetric algorithms are allowed."
                )
            if alg not in ALLOWED_ALGORITHMS:
                raise ValueError(
                    f"Algorithm '{alg}' is not supported. "
                    f"Allowed: {sorted(ALLOWED_ALGORITHMS)}"
                )

        # SSRF validation — fail fast at startup.
        if not _is_safe_url(jwks_url):
            raise ValueError(
                f"JWKS URL '{jwks_url}' fails SSRF validation. "
                "Use HTTPS, or HTTP only with localhost/127.0.0.1/::1."
            )

        self.jwks_url = jwks_url
        self.issuer = issuer
        self.audience = audience
        self.algorithms = algorithms
        self.clock_skew_seconds = clock_skew_seconds
        self.jwks_cache_ttl = jwks_cache_ttl

        self._client: httpx.AsyncClient | None = None
        self._jwks_cache: dict[str, jwt.PyJWK] = {}
        self._jwks_last_refresh: float = 0.0
        self._jwks_refresh_lock = asyncio.Lock()

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
        """Verify a JWT access token.

        Decodes the token header, looks up the signing key from the
        cached JWKS, verifies the signature and standard claims, then
        returns an ``AccessToken`` on success or ``None`` on any failure.

        Args:
            token: The JWT string to verify.

        Returns:
            AccessToken with extracted metadata if valid, None otherwise.
        """
        try:
            # 1. Decode header without verification to inspect alg/kid.
            header = jwt.get_unverified_header(token)
            alg: str = header.get("alg", "")
            kid: str | None = header.get("kid")

            # 2. Reject forbidden or unconfigured algorithms.
            if alg in _FORBIDDEN_ALGORITHMS:
                logger.warning("JWT uses forbidden algorithm: %s", alg)
                return None
            if alg not in self.algorithms:
                logger.debug(
                    "JWT algorithm '%s' not in allowed list %s",
                    alg,
                    self.algorithms,
                )
                return None

            # 3. Look up the signing key.
            signing_key = await self._get_signing_key(kid)
            if signing_key is None:
                logger.warning("No matching JWKS key for kid=%s", kid)
                return None

            # 4. Verify signature and validate claims.
            payload: dict[str, Any] = jwt.decode(
                token,
                signing_key.key,
                algorithms=self.algorithms,
                issuer=self.issuer,
                audience=self.audience,
                leeway=timedelta(seconds=self.clock_skew_seconds),
                options={"require": ["exp"]},
            )

            # 5. Extract client_id: azp > client_id > sub.
            client_id = (
                payload.get("azp")
                or payload.get("client_id")
                or payload.get("sub")
                or "unknown"
            )

            # 6. Extract scopes from space-delimited string (RFC 9068).
            scope_str: str = payload.get("scope", "")
            scopes = scope_str.split() if scope_str else []

            return AccessToken(
                token=token,
                client_id=client_id,
                scopes=scopes,
                expires_at=payload.get("exp"),
            )

        except jwt.ExpiredSignatureError:
            logger.debug("JWT token has expired")
            return None
        except jwt.InvalidAudienceError:
            logger.debug("JWT audience mismatch")
            return None
        except jwt.InvalidIssuerError:
            logger.debug("JWT issuer mismatch")
            return None
        except jwt.InvalidTokenError as e:
            logger.debug("JWT validation failed: %s", e)
            return None
        except Exception as e:
            logger.warning("JWT verification error: %s", e)
            return None

    # ------------------------------------------------------------------
    # JWKS key management
    # ------------------------------------------------------------------

    async def _get_signing_key(self, kid: str | None) -> jwt.PyJWK | None:
        """Look up a signing key from the JWKS cache, refreshing if needed.

        Uses an asyncio.Lock to prevent thundering herd on concurrent requests
        when the cache is empty or stale. Multiple callers will wait for a
        single refresh rather than all hitting the JWKS endpoint simultaneously.

        Args:
            kid: The key ID from the JWT header, or None.

        Returns:
            The matching PyJWK, or None if not found.
        """
        # Ensure we have keys (initial fetch or stale cache).
        # Use lock to prevent thundering herd when cache is empty.
        if not self._jwks_cache or self._is_cache_stale():
            async with self._jwks_refresh_lock:
                # Double-check after acquiring lock (another task may have refreshed).
                if not self._jwks_cache or self._is_cache_stale():
                    await self._refresh_jwks()

        if kid:
            # Try cache hit.
            if kid in self._jwks_cache:
                return self._jwks_cache[kid]

            # Cache miss — possible key rotation; try one refresh (rate-limited).
            async with self._jwks_refresh_lock:
                # Double-check: another task may have refreshed while we waited.
                if kid in self._jwks_cache:
                    return self._jwks_cache[kid]
                if self._can_refresh():
                    await self._refresh_jwks()
                    return self._jwks_cache.get(kid)

            logger.debug("JWKS refresh rate-limited; kid '%s' not in cache", kid)
            return None

        # No kid: accept only if the JWKS has exactly one key.
        if len(self._jwks_cache) == 1:
            return next(iter(self._jwks_cache.values()))

        logger.debug("JWT has no kid and JWKS contains %d keys", len(self._jwks_cache))
        return None

    async def _refresh_jwks(self) -> None:
        """Fetch the JWKS from the endpoint and update the cache."""
        client = self._get_client()
        try:
            response = await client.get(self.jwks_url)
            response.raise_for_status()
            data = response.json()

            keys_data: list[dict[str, Any]] = data.get("keys", [])
            if not keys_data:
                logger.warning("JWKS response contains no keys")
                return

            new_cache: dict[str, jwt.PyJWK] = {}
            for key_data in keys_data:
                try:
                    jwk = jwt.PyJWK(key_data)
                    key_id = key_data.get("kid")
                    if key_id:
                        new_cache[key_id] = jwk
                except Exception as e:
                    logger.debug("Skipping invalid JWK entry: %s", e)

            if new_cache:
                self._jwks_cache = new_cache
                logger.debug("JWKS cache updated with %d keys", len(new_cache))
            else:
                logger.warning("No valid keys found in JWKS response")

        except httpx.TimeoutException:
            logger.warning("JWKS fetch timeout from %s", self.jwks_url)
        except httpx.HTTPStatusError as e:
            logger.warning("JWKS fetch HTTP error: %s", e)
        except Exception as e:
            logger.warning("JWKS fetch failed: %s", e)
        finally:
            self._jwks_last_refresh = time.monotonic()

    def _is_cache_stale(self) -> bool:
        """Return True if the JWKS cache has exceeded its TTL."""
        return (time.monotonic() - self._jwks_last_refresh) > self.jwks_cache_ttl

    def _can_refresh(self) -> bool:
        """Return True if enough time has passed since the last refresh."""
        return (time.monotonic() - self._jwks_last_refresh) > _JWKS_MIN_REFRESH_INTERVAL
