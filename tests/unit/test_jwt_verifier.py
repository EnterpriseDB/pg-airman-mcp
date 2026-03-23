"""Unit tests for JwtTokenVerifier JWT validation."""
# pyright: reportPrivateUsage=false

import json
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import jwt as pyjwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm

from pg_airman_mcp.jwt_verifier import JwtTokenVerifier

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def rsa_private_key() -> rsa.RSAPrivateKey:
    """Generate an RSA private key for signing test JWTs."""
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture()
def rsa_public_key(
    rsa_private_key: rsa.RSAPrivateKey,
) -> rsa.RSAPublicKey:
    """Derive the RSA public key from the private key."""
    return rsa_private_key.public_key()


@pytest.fixture()
def jwks_data(rsa_public_key: rsa.RSAPublicKey) -> dict[str, Any]:
    """Build a JWKS response containing the test public key."""
    jwk_dict: dict[str, Any] = json.loads(RSAAlgorithm.to_jwk(rsa_public_key))
    jwk_dict["kid"] = "test-key-1"
    jwk_dict["use"] = "sig"
    return {"keys": [jwk_dict]}


@pytest.fixture()
def _make_token(rsa_private_key: rsa.RSAPrivateKey):
    """Return a factory that creates signed JWTs with sensible defaults."""

    def _factory(
        claims: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> str:
        base_claims: dict[str, Any] = {
            "iss": "https://auth.example.com",
            "aud": "https://mcp.example.com",
            "exp": int(time.time()) + 3600,
            "azp": "my-client",
            "scope": "read write",
        }
        if claims:
            base_claims.update(claims)
        base_headers: dict[str, str] = {"kid": "test-key-1"}
        if headers:
            base_headers.update(headers)
        return pyjwt.encode(
            base_claims,
            rsa_private_key,
            algorithm="RS256",
            headers=base_headers,
        )

    return _factory


@pytest.fixture()
def verifier() -> JwtTokenVerifier:
    """Create a JwtTokenVerifier with standard test parameters."""
    return JwtTokenVerifier(
        jwks_url="https://auth.example.com/.well-known/jwks.json",
        issuer="https://auth.example.com",
        audience="https://mcp.example.com",
    )


def _mock_jwks_client(
    verifier: JwtTokenVerifier, jwks_data: dict[str, Any]
) -> AsyncMock:
    """Inject a mock httpx client that returns the given JWKS data."""
    mock_response: MagicMock = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = jwks_data
    mock_response.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response
    mock_client.is_closed = False
    verifier._client = mock_client
    return mock_client


# ---------------------------------------------------------------------------
# 1. Constructor validation
# ---------------------------------------------------------------------------


class TestConstructorValidation:
    """Test constructor argument validation for algorithms and SSRF."""

    @pytest.mark.parametrize("alg", ["none", "HS256", "HS384", "HS512"])
    def test_forbidden_algorithm_raises(self, alg: str) -> None:
        """Forbidden symmetric/none algorithms are rejected."""
        with pytest.raises(ValueError, match="forbidden"):
            JwtTokenVerifier(
                jwks_url="https://auth.example.com/jwks",
                issuer="https://auth.example.com",
                audience="aud",
                algorithms=[alg],
            )

    @pytest.mark.parametrize("alg", ["PS256", "EdDSA", "BOGUS"])
    def test_unsupported_algorithm_raises(self, alg: str) -> None:
        """Algorithms not in the allowed set are rejected."""
        with pytest.raises(ValueError, match="not supported"):
            JwtTokenVerifier(
                jwks_url="https://auth.example.com/jwks",
                issuer="https://auth.example.com",
                audience="aud",
                algorithms=[alg],
            )

    @pytest.mark.parametrize("alg", ["RS256", "RS384", "RS512", "ES256"])
    def test_valid_algorithm_accepted(self, alg: str) -> None:
        """Supported asymmetric algorithms are accepted."""
        v = JwtTokenVerifier(
            jwks_url="https://auth.example.com/jwks",
            issuer="https://auth.example.com",
            audience="aud",
            algorithms=[alg],
        )
        assert alg in v.algorithms

    def test_default_algorithm_is_rs256(self) -> None:
        """When no algorithms are specified, RS256 is the default."""
        v = JwtTokenVerifier(
            jwks_url="https://auth.example.com/jwks",
            issuer="https://auth.example.com",
            audience="aud",
        )
        assert v.algorithms == ["RS256"]

    @pytest.mark.parametrize(
        "url",
        [
            "http://evil.com/.well-known/jwks.json",
            "ftp://localhost/.well-known/jwks.json",
            "http://10.0.0.1/.well-known/jwks.json",
            "http://localhost.evil.com/.well-known/jwks.json",
            "http://192.168.1.1/jwks",
        ],
    )
    def test_ssrf_rejects_unsafe_urls(self, url: str) -> None:
        """Unsafe JWKS URLs are rejected at construction time."""
        with pytest.raises(ValueError, match="SSRF"):
            JwtTokenVerifier(
                jwks_url=url,
                issuer="https://auth.example.com",
                audience="aud",
            )

    @pytest.mark.parametrize(
        "url",
        [
            "https://any.com/.well-known/jwks.json",
            "https://auth.evil.com/jwks",
            "http://localhost/.well-known/jwks.json",
            "http://127.0.0.1/.well-known/jwks.json",
            "http://[::1]/.well-known/jwks.json",
        ],
    )
    def test_ssrf_allows_safe_urls(self, url: str) -> None:
        """HTTPS and exact localhost variants pass SSRF validation."""
        v = JwtTokenVerifier(
            jwks_url=url,
            issuer="https://auth.example.com",
            audience="aud",
        )
        assert v.jwks_url == url


# ---------------------------------------------------------------------------
# 2. verify_token with valid token
# ---------------------------------------------------------------------------


class TestVerifyTokenValid:
    """Test successful token verification with a real RS256 JWT."""

    @pytest.mark.asyncio
    async def test_valid_token_returns_access_token(
        self, verifier: JwtTokenVerifier, jwks_data: dict[str, Any], _make_token
    ) -> None:
        """A correctly signed JWT with valid claims returns an AccessToken."""
        _mock_jwks_client(verifier, jwks_data)
        token = _make_token()

        result = await verifier.verify_token(token)

        assert result is not None
        assert result.token == token
        assert result.client_id == "my-client"
        assert result.scopes == ["read", "write"]
        assert result.expires_at is not None


# ---------------------------------------------------------------------------
# 3. Client ID extraction priority
# ---------------------------------------------------------------------------


class TestClientIdExtraction:
    """Test client_id extraction from claims with correct priority."""

    @pytest.mark.asyncio
    async def test_azp_takes_priority(
        self, verifier: JwtTokenVerifier, jwks_data: dict[str, Any], _make_token
    ) -> None:
        """When azp is present, it is used as client_id."""
        _mock_jwks_client(verifier, jwks_data)
        token = _make_token(
            claims={"azp": "azp-client", "client_id": "cid", "sub": "subj"}
        )

        result = await verifier.verify_token(token)

        assert result is not None
        assert result.client_id == "azp-client"

    @pytest.mark.asyncio
    async def test_client_id_claim_when_no_azp(
        self, verifier: JwtTokenVerifier, jwks_data: dict[str, Any], _make_token
    ) -> None:
        """When azp is absent, client_id claim is used."""
        _mock_jwks_client(verifier, jwks_data)
        token = _make_token(
            claims={"azp": None, "client_id": "cid-client", "sub": "subj"}
        )

        result = await verifier.verify_token(token)

        assert result is not None
        assert result.client_id == "cid-client"

    @pytest.mark.asyncio
    async def test_sub_when_no_azp_or_client_id(
        self, verifier: JwtTokenVerifier, jwks_data: dict[str, Any], _make_token
    ) -> None:
        """When azp and client_id are absent, sub is used."""
        _mock_jwks_client(verifier, jwks_data)
        # Explicitly remove azp and client_id by setting to None/empty.
        token = _make_token(
            claims={"azp": None, "client_id": None, "sub": "subject-user"}
        )

        result = await verifier.verify_token(token)

        assert result is not None
        assert result.client_id == "subject-user"

    @pytest.mark.asyncio
    async def test_unknown_when_no_identity_claims(
        self, verifier: JwtTokenVerifier, jwks_data: dict[str, Any], rsa_private_key
    ) -> None:
        """When no identity claims are present, 'unknown' is returned."""
        _mock_jwks_client(verifier, jwks_data)
        # Build a token with no azp, client_id, or sub claims at all.
        token = pyjwt.encode(
            {
                "iss": "https://auth.example.com",
                "aud": "https://mcp.example.com",
                "exp": int(time.time()) + 3600,
            },
            rsa_private_key,
            algorithm="RS256",
            headers={"kid": "test-key-1"},
        )

        result = await verifier.verify_token(token)

        assert result is not None
        assert result.client_id == "unknown"


# ---------------------------------------------------------------------------
# 4. Scope extraction
# ---------------------------------------------------------------------------


class TestScopeExtraction:
    """Test scope parsing from the space-delimited scope claim."""

    @pytest.mark.asyncio
    async def test_space_delimited_scopes(
        self, verifier: JwtTokenVerifier, jwks_data: dict[str, Any], _make_token
    ) -> None:
        """Space-delimited scope string is split into a list."""
        _mock_jwks_client(verifier, jwks_data)
        token = _make_token(claims={"scope": "read write admin"})

        result = await verifier.verify_token(token)

        assert result is not None
        assert result.scopes == ["read", "write", "admin"]

    @pytest.mark.asyncio
    async def test_empty_scope_string(
        self, verifier: JwtTokenVerifier, jwks_data: dict[str, Any], _make_token
    ) -> None:
        """Empty scope string produces an empty list."""
        _mock_jwks_client(verifier, jwks_data)
        token = _make_token(claims={"scope": ""})

        result = await verifier.verify_token(token)

        assert result is not None
        assert result.scopes == []

    @pytest.mark.asyncio
    async def test_no_scope_claim(
        self, verifier: JwtTokenVerifier, jwks_data: dict[str, Any], rsa_private_key
    ) -> None:
        """Missing scope claim produces an empty list."""
        _mock_jwks_client(verifier, jwks_data)
        # Build a token without the scope key at all.
        token = pyjwt.encode(
            {
                "iss": "https://auth.example.com",
                "aud": "https://mcp.example.com",
                "exp": int(time.time()) + 3600,
                "azp": "client",
            },
            rsa_private_key,
            algorithm="RS256",
            headers={"kid": "test-key-1"},
        )

        result = await verifier.verify_token(token)

        assert result is not None
        assert result.scopes == []


# ---------------------------------------------------------------------------
# 5. Token rejection
# ---------------------------------------------------------------------------


class TestTokenRejection:
    """Test that invalid tokens are correctly rejected (return None)."""

    @pytest.mark.asyncio
    async def test_expired_token(
        self, verifier: JwtTokenVerifier, jwks_data: dict[str, Any], _make_token
    ) -> None:
        """Expired token returns None."""
        _mock_jwks_client(verifier, jwks_data)
        token = _make_token(claims={"exp": int(time.time()) - 3600})

        result = await verifier.verify_token(token)

        assert result is None

    @pytest.mark.asyncio
    async def test_wrong_issuer(
        self, verifier: JwtTokenVerifier, jwks_data: dict[str, Any], _make_token
    ) -> None:
        """Token with wrong issuer returns None."""
        _mock_jwks_client(verifier, jwks_data)
        token = _make_token(claims={"iss": "https://wrong-issuer.com"})

        result = await verifier.verify_token(token)

        assert result is None

    @pytest.mark.asyncio
    async def test_wrong_audience(
        self, verifier: JwtTokenVerifier, jwks_data: dict[str, Any], _make_token
    ) -> None:
        """Token with wrong audience returns None."""
        _mock_jwks_client(verifier, jwks_data)
        token = _make_token(claims={"aud": "https://wrong-audience.com"})

        result = await verifier.verify_token(token)

        assert result is None

    @pytest.mark.asyncio
    async def test_forbidden_algorithm_in_header(
        self, verifier: JwtTokenVerifier, jwks_data: dict[str, Any]
    ) -> None:
        """JWT signed with forbidden algorithm (HS256) returns None."""
        _mock_jwks_client(verifier, jwks_data)
        # Create an HS256-signed token (symmetric — should be rejected).
        token = pyjwt.encode(
            {
                "iss": "https://auth.example.com",
                "aud": "https://mcp.example.com",
                "exp": int(time.time()) + 3600,
            },
            "secret-key",
            algorithm="HS256",
        )

        result = await verifier.verify_token(token)

        assert result is None

    @pytest.mark.asyncio
    async def test_algorithm_not_in_configured_list(
        self,
        jwks_data: dict[str, Any],
        rsa_private_key: rsa.RSAPrivateKey,
    ) -> None:
        """JWT using RS384 rejected when verifier only allows RS256."""
        v = JwtTokenVerifier(
            jwks_url="https://auth.example.com/jwks",
            issuer="https://auth.example.com",
            audience="https://mcp.example.com",
            algorithms=["RS256"],  # Only RS256 allowed.
        )
        _mock_jwks_client(v, jwks_data)

        # Sign with RS384 — not in the allowed list.
        token = pyjwt.encode(
            {
                "iss": "https://auth.example.com",
                "aud": "https://mcp.example.com",
                "exp": int(time.time()) + 3600,
            },
            rsa_private_key,
            algorithm="RS384",
            headers={"kid": "test-key-1"},
        )

        result = await v.verify_token(token)

        assert result is None

    @pytest.mark.asyncio
    async def test_missing_exp_claim_rejected(
        self,
        verifier: JwtTokenVerifier,
        jwks_data: dict[str, Any],
        rsa_private_key: rsa.RSAPrivateKey,
    ) -> None:
        """Token without exp claim is rejected (fail-closed)."""
        _mock_jwks_client(verifier, jwks_data)
        token = pyjwt.encode(
            {
                "iss": "https://auth.example.com",
                "aud": "https://mcp.example.com",
                "azp": "my-client",
                # No "exp" claim.
            },
            rsa_private_key,
            algorithm="RS256",
            headers={"kid": "test-key-1"},
        )

        result = await verifier.verify_token(token)

        assert result is None


# ---------------------------------------------------------------------------
# 6. JWKS key management
# ---------------------------------------------------------------------------


class TestJwksKeyManagement:
    """Test JWKS caching, refresh, and key lookup behaviour."""

    @pytest.mark.asyncio
    async def test_unknown_kid_triggers_refresh(
        self, verifier: JwtTokenVerifier, jwks_data: dict[str, Any], _make_token
    ) -> None:
        """An unknown kid causes a JWKS refresh attempt."""
        mock_client = _mock_jwks_client(verifier, jwks_data)
        # Seed the cache with a stale kid so first fetch happens.
        verifier._jwks_last_refresh = 0.0

        token = _make_token(headers={"kid": "test-key-1"})
        result = await verifier.verify_token(token)

        assert result is not None
        # At least one GET call was made to fetch JWKS.
        assert mock_client.get.call_count >= 1

    @pytest.mark.asyncio
    async def test_unknown_kid_triggers_second_refresh(
        self,
        verifier: JwtTokenVerifier,
        rsa_public_key: rsa.RSAPublicKey,
        _make_token,
    ) -> None:
        """When kid is not in cache, a refresh is attempted to find it."""
        # Build JWKS with a different kid than what's in cache initially.
        new_jwk = json.loads(RSAAlgorithm.to_jwk(rsa_public_key))
        new_jwk["kid"] = "rotated-key"
        new_jwk["use"] = "sig"
        new_jwks = {"keys": [new_jwk]}

        mock_client = _mock_jwks_client(verifier, new_jwks)
        # Pre-populate cache with an old key.
        verifier._jwks_cache = {"old-key": MagicMock()}
        verifier._jwks_last_refresh = 0.0  # Allow refresh.

        token = _make_token(headers={"kid": "rotated-key"})
        result = await verifier.verify_token(token)

        assert result is not None
        # Refresh happened at least once.
        assert mock_client.get.call_count >= 1

    @pytest.mark.asyncio
    async def test_refresh_rate_limiting(
        self, verifier: JwtTokenVerifier, jwks_data: dict[str, Any], _make_token
    ) -> None:
        """JWKS refresh is rate-limited when called too frequently."""
        mock_client = _mock_jwks_client(verifier, jwks_data)

        # Seed cache with a key that won't match.
        verifier._jwks_cache = {"other-kid": MagicMock()}
        # Set last refresh to very recently (within rate limit window).
        verifier._jwks_last_refresh = time.monotonic() - 1.0

        token = _make_token(headers={"kid": "unknown-kid"})
        result = await verifier.verify_token(token)

        # Should return None because refresh was rate-limited.
        assert result is None
        # No fetch because rate limit prevents it (cache not stale either).
        mock_client.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_kid_single_key_accepted(
        self, verifier: JwtTokenVerifier, jwks_data: dict[str, Any], rsa_private_key
    ) -> None:
        """JWT without kid is accepted when JWKS has exactly one key."""
        _mock_jwks_client(verifier, jwks_data)
        # Encode a token without a kid header.
        token = pyjwt.encode(
            {
                "iss": "https://auth.example.com",
                "aud": "https://mcp.example.com",
                "exp": int(time.time()) + 3600,
                "azp": "my-client",
                "scope": "read",
            },
            rsa_private_key,
            algorithm="RS256",
            # No kid in headers — tests the single-key fallback path.
        )

        result = await verifier.verify_token(token)

        assert result is not None

    @pytest.mark.asyncio
    async def test_no_kid_multiple_keys_rejected(
        self,
        verifier: JwtTokenVerifier,
        rsa_public_key: rsa.RSAPublicKey,
        rsa_private_key: rsa.RSAPrivateKey,
    ) -> None:
        """JWT without kid is rejected when JWKS has multiple keys."""
        # Build JWKS with two keys.
        jwk1 = json.loads(RSAAlgorithm.to_jwk(rsa_public_key))
        jwk1["kid"] = "key-1"
        jwk1["use"] = "sig"
        jwk2 = json.loads(RSAAlgorithm.to_jwk(rsa_public_key))
        jwk2["kid"] = "key-2"
        jwk2["use"] = "sig"
        multi_jwks = {"keys": [jwk1, jwk2]}

        _mock_jwks_client(verifier, multi_jwks)

        # Token without kid header.
        token = pyjwt.encode(
            {
                "iss": "https://auth.example.com",
                "aud": "https://mcp.example.com",
                "exp": int(time.time()) + 3600,
                "azp": "my-client",
            },
            rsa_private_key,
            algorithm="RS256",
        )

        result = await verifier.verify_token(token)

        assert result is None


# ---------------------------------------------------------------------------
# 7. Client lifecycle
# ---------------------------------------------------------------------------


class TestClientLifecycle:
    """Test that the shared HTTP client is managed correctly."""

    def test_client_created_lazily(self, verifier: JwtTokenVerifier) -> None:
        """Client is None until first use."""
        assert verifier._client is None

    def test_get_client_creates_client(self, verifier: JwtTokenVerifier) -> None:
        """_get_client() creates and returns an httpx.AsyncClient."""
        client = verifier._get_client()
        assert isinstance(client, httpx.AsyncClient)
        assert not client.is_closed

    def test_get_client_reuses_existing(self, verifier: JwtTokenVerifier) -> None:
        """_get_client() returns the same instance on subsequent calls."""
        client1 = verifier._get_client()
        client2 = verifier._get_client()
        assert client1 is client2

    @pytest.mark.asyncio
    async def test_close_calls_aclose(self, verifier: JwtTokenVerifier) -> None:
        """close() calls aclose() on the underlying client."""
        mock_client = AsyncMock()
        mock_client.is_closed = False
        verifier._client = mock_client

        await verifier.close()

        mock_client.aclose.assert_awaited_once()
        assert verifier._client is None

    @pytest.mark.asyncio
    async def test_close_safe_when_no_client(self, verifier: JwtTokenVerifier) -> None:
        """close() is safe to call when no client was ever created."""
        await verifier.close()  # Should not raise.

    @pytest.mark.asyncio
    async def test_close_safe_when_client_already_closed(
        self, verifier: JwtTokenVerifier
    ) -> None:
        """close() is safe when client is already closed."""
        mock_client = AsyncMock()
        mock_client.is_closed = True
        verifier._client = mock_client

        await verifier.close()  # Should not raise.

        mock_client.aclose.assert_not_awaited()


# ---------------------------------------------------------------------------
# 8. JWKS fetch error handling
# ---------------------------------------------------------------------------


class TestJwksFetchErrors:
    """Test error handling during JWKS endpoint fetches."""

    @pytest.mark.asyncio
    async def test_timeout_returns_none(
        self, verifier: JwtTokenVerifier, _make_token
    ) -> None:
        """JWKS timeout leaves cache empty and verify_token returns None."""
        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx.TimeoutException("timed out")
        mock_client.is_closed = False
        verifier._client = mock_client

        token = _make_token()
        result = await verifier.verify_token(token)

        assert result is None
        assert verifier._jwks_cache == {}

    @pytest.mark.asyncio
    async def test_http_error_returns_none(
        self, verifier: JwtTokenVerifier, _make_token
    ) -> None:
        """JWKS HTTP error leaves cache empty and verify_token returns None."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Server Error",
            request=MagicMock(),
            response=mock_response,
        )

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        mock_client.is_closed = False
        verifier._client = mock_client

        token = _make_token()
        result = await verifier.verify_token(token)

        assert result is None
        assert verifier._jwks_cache == {}

    @pytest.mark.asyncio
    async def test_empty_keys_array_does_not_update_cache(
        self, verifier: JwtTokenVerifier, _make_token
    ) -> None:
        """JWKS response with empty keys array does not update the cache."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"keys": []}
        mock_response.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        mock_client.is_closed = False
        verifier._client = mock_client

        # Pre-populate with a sentinel to verify it's not replaced.
        sentinel = MagicMock()
        verifier._jwks_cache = {"existing": sentinel}
        verifier._jwks_last_refresh = 0.0  # Force stale.

        token = _make_token()
        await verifier.verify_token(token)

        # Cache should retain the old value (empty keys didn't overwrite).
        assert "existing" in verifier._jwks_cache
        assert verifier._jwks_cache["existing"] is sentinel

    @pytest.mark.asyncio
    async def test_jwks_last_refresh_updated_even_on_error(
        self, verifier: JwtTokenVerifier
    ) -> None:
        """The _jwks_last_refresh timestamp is set even when fetch fails."""
        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx.TimeoutException("timed out")
        mock_client.is_closed = False
        verifier._client = mock_client
        verifier._jwks_last_refresh = 0.0

        before = time.monotonic()
        await verifier._refresh_jwks()
        after = time.monotonic()

        assert verifier._jwks_last_refresh >= before
        assert verifier._jwks_last_refresh <= after
