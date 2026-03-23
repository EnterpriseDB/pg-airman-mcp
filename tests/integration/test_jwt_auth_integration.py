"""Integration tests for JWT authentication flow.

These tests validate the full JWT auth pipeline: key generation → token
signing → JWKS endpoint serving → JwtTokenVerifier verification. Unlike
the unit tests (which inject a mock httpx client), these tests use
``respx`` to intercept HTTP requests at the transport layer, letting
the verifier's real httpx client run its full code path.

Test scenarios from POS-85:
- Valid token accepted with correct AccessToken fields
- Expired token rejected
- Wrong issuer rejected
- Wrong audience rejected
- Disallowed algorithm rejected
- Key rotation: new key picked up via JWKS refresh
- Scope enforcement
- Auth type coexistence (introspection ↔ jwt config switching)
"""

# pyright: reportPrivateUsage=false

import json
import time
from typing import Any

import jwt as pyjwt
import pytest
import respx
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from jwt.algorithms import ECAlgorithm, RSAAlgorithm

from pg_airman_mcp.auth_config import AuthContext, JwtConfig, create_auth_context
from pg_airman_mcp.jwt_verifier import JwtTokenVerifier

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ISSUER = "https://auth.example.com"
AUDIENCE = "https://mcp.example.com"
JWKS_URL = f"{ISSUER}/.well-known/jwks.json"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _rsa_keypair() -> tuple[rsa.RSAPrivateKey, rsa.RSAPublicKey]:
    """Generate an RSA key pair."""
    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return private, private.public_key()


def _ec_keypair() -> tuple[ec.EllipticCurvePrivateKey, ec.EllipticCurvePublicKey]:
    """Generate an EC P-256 key pair."""
    private = ec.generate_private_key(ec.SECP256R1())
    return private, private.public_key()


def _build_jwks(*keys: tuple[Any, str, str]) -> dict[str, Any]:
    """Build a JWKS dict from (public_key, kid, alg) tuples."""
    jwk_list: list[dict[str, Any]] = []
    for pub_key, kid, alg in keys:
        if alg.startswith("RS"):
            jwk_dict = json.loads(RSAAlgorithm.to_jwk(pub_key))
        elif alg.startswith("ES"):
            jwk_dict = json.loads(ECAlgorithm.to_jwk(pub_key))
        else:
            raise ValueError(f"Unsupported algorithm family: {alg}")
        jwk_dict["kid"] = kid
        jwk_dict["use"] = "sig"
        return_dict: dict[str, Any] = jwk_dict
        jwk_list.append(return_dict)
    return {"keys": jwk_list}


def _sign_token(
    private_key: Any,
    kid: str,
    algorithm: str = "RS256",
    claims: dict[str, Any] | None = None,
) -> str:
    """Sign a JWT with sensible defaults."""
    base: dict[str, Any] = {
        "iss": ISSUER,
        "aud": AUDIENCE,
        "exp": int(time.time()) + 3600,
        "azp": "test-client",
        "scope": "mcp:postgres:access read write",
    }
    if claims:
        base.update(claims)
    return pyjwt.encode(base, private_key, algorithm=algorithm, headers={"kid": kid})


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def rsa_keys() -> tuple[rsa.RSAPrivateKey, rsa.RSAPublicKey]:
    """RSA key pair for signing test tokens."""
    return _rsa_keypair()


@pytest.fixture()
def ec_keys() -> tuple[ec.EllipticCurvePrivateKey, ec.EllipticCurvePublicKey]:
    """EC P-256 key pair for signing test tokens."""
    return _ec_keypair()


# ---------------------------------------------------------------------------
# 1. Full flow: valid token through JwtTokenVerifier
# ---------------------------------------------------------------------------


class TestValidTokenFlow:
    """End-to-end: sign a token, serve JWKS, verify via JwtTokenVerifier."""

    @pytest.mark.asyncio
    async def test_valid_rsa_token_accepted(
        self, rsa_keys: tuple[rsa.RSAPrivateKey, rsa.RSAPublicKey]
    ) -> None:
        """Valid RS256 token is accepted with correct AccessToken fields."""
        private_key, public_key = rsa_keys
        jwks = _build_jwks((public_key, "rsa-key-1", "RS256"))
        token = _sign_token(private_key, "rsa-key-1")

        with respx.mock:
            respx.get(JWKS_URL).respond(json=jwks)

            verifier = JwtTokenVerifier(
                jwks_url=JWKS_URL,
                issuer=ISSUER,
                audience=AUDIENCE,
            )
            try:
                result = await verifier.verify_token(token)

                assert result is not None
                assert result.token == token
                assert result.client_id == "test-client"
                assert "mcp:postgres:access" in result.scopes
                assert "read" in result.scopes
                assert "write" in result.scopes
                assert result.expires_at is not None
            finally:
                await verifier.close()

    @pytest.mark.asyncio
    async def test_valid_ec_token_accepted(
        self, ec_keys: tuple[ec.EllipticCurvePrivateKey, ec.EllipticCurvePublicKey]
    ) -> None:
        """Valid ES256 token is accepted via EC key pair."""
        private_key, public_key = ec_keys
        jwks = _build_jwks((public_key, "ec-key-1", "ES256"))
        token = _sign_token(private_key, "ec-key-1", algorithm="ES256")

        with respx.mock:
            respx.get(JWKS_URL).respond(json=jwks)

            verifier = JwtTokenVerifier(
                jwks_url=JWKS_URL,
                issuer=ISSUER,
                audience=AUDIENCE,
                algorithms=["ES256"],
            )
            try:
                result = await verifier.verify_token(token)

                assert result is not None
                assert result.client_id == "test-client"
            finally:
                await verifier.close()


# ---------------------------------------------------------------------------
# 2. Token rejection scenarios
# ---------------------------------------------------------------------------


class TestTokenRejection:
    """Tokens that should be rejected by the full verification pipeline."""

    @pytest.mark.asyncio
    async def test_expired_token_rejected(
        self, rsa_keys: tuple[rsa.RSAPrivateKey, rsa.RSAPublicKey]
    ) -> None:
        """Expired token returns None."""
        private_key, public_key = rsa_keys
        jwks = _build_jwks((public_key, "key-1", "RS256"))
        token = _sign_token(
            private_key, "key-1", claims={"exp": int(time.time()) - 3600}
        )

        with respx.mock:
            respx.get(JWKS_URL).respond(json=jwks)

            verifier = JwtTokenVerifier(
                jwks_url=JWKS_URL, issuer=ISSUER, audience=AUDIENCE
            )
            try:
                result = await verifier.verify_token(token)
                assert result is None
            finally:
                await verifier.close()

    @pytest.mark.asyncio
    async def test_wrong_issuer_rejected(
        self, rsa_keys: tuple[rsa.RSAPrivateKey, rsa.RSAPublicKey]
    ) -> None:
        """Token with mismatched issuer returns None."""
        private_key, public_key = rsa_keys
        jwks = _build_jwks((public_key, "key-1", "RS256"))
        token = _sign_token(
            private_key, "key-1", claims={"iss": "https://wrong-issuer.com"}
        )

        with respx.mock:
            respx.get(JWKS_URL).respond(json=jwks)

            verifier = JwtTokenVerifier(
                jwks_url=JWKS_URL, issuer=ISSUER, audience=AUDIENCE
            )
            try:
                result = await verifier.verify_token(token)
                assert result is None
            finally:
                await verifier.close()

    @pytest.mark.asyncio
    async def test_wrong_audience_rejected(
        self, rsa_keys: tuple[rsa.RSAPrivateKey, rsa.RSAPublicKey]
    ) -> None:
        """Token with mismatched audience returns None."""
        private_key, public_key = rsa_keys
        jwks = _build_jwks((public_key, "key-1", "RS256"))
        token = _sign_token(
            private_key, "key-1", claims={"aud": "https://wrong-audience.com"}
        )

        with respx.mock:
            respx.get(JWKS_URL).respond(json=jwks)

            verifier = JwtTokenVerifier(
                jwks_url=JWKS_URL, issuer=ISSUER, audience=AUDIENCE
            )
            try:
                result = await verifier.verify_token(token)
                assert result is None
            finally:
                await verifier.close()

    @pytest.mark.asyncio
    async def test_disallowed_algorithm_rejected(
        self, rsa_keys: tuple[rsa.RSAPrivateKey, rsa.RSAPublicKey]
    ) -> None:
        """Token signed with RS384 rejected when verifier only allows RS256."""
        private_key, public_key = rsa_keys
        jwks = _build_jwks((public_key, "key-1", "RS256"))
        token = _sign_token(private_key, "key-1", algorithm="RS384")

        with respx.mock:
            respx.get(JWKS_URL).respond(json=jwks)

            verifier = JwtTokenVerifier(
                jwks_url=JWKS_URL,
                issuer=ISSUER,
                audience=AUDIENCE,
                algorithms=["RS256"],
            )
            try:
                result = await verifier.verify_token(token)
                assert result is None
            finally:
                await verifier.close()

    @pytest.mark.asyncio
    async def test_tampered_token_rejected(
        self, rsa_keys: tuple[rsa.RSAPrivateKey, rsa.RSAPublicKey]
    ) -> None:
        """Token signed with a different key is rejected."""
        _, public_key = rsa_keys
        jwks = _build_jwks((public_key, "key-1", "RS256"))

        # Sign with a completely different key.
        other_private, _ = _rsa_keypair()
        token = _sign_token(other_private, "key-1")

        with respx.mock:
            respx.get(JWKS_URL).respond(json=jwks)

            verifier = JwtTokenVerifier(
                jwks_url=JWKS_URL, issuer=ISSUER, audience=AUDIENCE
            )
            try:
                result = await verifier.verify_token(token)
                assert result is None
            finally:
                await verifier.close()


# ---------------------------------------------------------------------------
# 3. Key rotation
# ---------------------------------------------------------------------------


class TestKeyRotation:
    """Test that JWKS refresh picks up rotated keys."""

    @pytest.mark.asyncio
    async def test_key_rotation_triggers_jwks_refresh(self) -> None:
        """When a token uses a new kid, JWKS is refreshed and key found."""
        # Phase 1: original key pair.
        old_private, old_public = _rsa_keypair()
        old_jwks = _build_jwks((old_public, "old-key", "RS256"))

        # Phase 2: rotated key pair.
        new_private, new_public = _rsa_keypair()
        new_jwks = _build_jwks((new_public, "new-key", "RS256"))

        with respx.mock:
            # First JWKS fetch returns old key.
            route = respx.get(JWKS_URL).respond(json=old_jwks)

            verifier = JwtTokenVerifier(
                jwks_url=JWKS_URL, issuer=ISSUER, audience=AUDIENCE
            )
            try:
                # Verify a token signed with the old key — should succeed.
                old_token = _sign_token(old_private, "old-key")
                result = await verifier.verify_token(old_token)
                assert result is not None

                # Simulate key rotation: JWKS now serves the new key.
                route.side_effect = None
                route.respond(json=new_jwks)

                # Allow refresh (bypass rate limit).
                verifier._jwks_last_refresh = 0.0

                # Verify a token signed with the new key.
                new_token = _sign_token(new_private, "new-key")
                result = await verifier.verify_token(new_token)
                assert result is not None
                assert result.client_id == "test-client"
            finally:
                await verifier.close()

    @pytest.mark.asyncio
    async def test_old_key_rejected_after_rotation(self) -> None:
        """After key rotation, tokens signed with the old key are rejected."""
        old_private, _ = _rsa_keypair()
        _, new_public = _rsa_keypair()
        new_jwks = _build_jwks((new_public, "new-key", "RS256"))

        with respx.mock:
            # JWKS only serves the new key.
            respx.get(JWKS_URL).respond(json=new_jwks)

            verifier = JwtTokenVerifier(
                jwks_url=JWKS_URL, issuer=ISSUER, audience=AUDIENCE
            )
            try:
                # Token signed with old key (kid doesn't match new JWKS).
                old_token = _sign_token(old_private, "old-key")
                result = await verifier.verify_token(old_token)
                assert result is None
            finally:
                await verifier.close()


# ---------------------------------------------------------------------------
# 4. Scope enforcement
# ---------------------------------------------------------------------------


class TestScopeEnforcement:
    """Test scope extraction and verification."""

    @pytest.mark.asyncio
    async def test_scopes_correctly_extracted(
        self, rsa_keys: tuple[rsa.RSAPrivateKey, rsa.RSAPublicKey]
    ) -> None:
        """Scopes from the token are correctly split and returned."""
        private_key, public_key = rsa_keys
        jwks = _build_jwks((public_key, "key-1", "RS256"))
        token = _sign_token(private_key, "key-1", claims={"scope": "admin read write"})

        with respx.mock:
            respx.get(JWKS_URL).respond(json=jwks)

            verifier = JwtTokenVerifier(
                jwks_url=JWKS_URL, issuer=ISSUER, audience=AUDIENCE
            )
            try:
                result = await verifier.verify_token(token)
                assert result is not None
                assert result.scopes == ["admin", "read", "write"]
            finally:
                await verifier.close()

    @pytest.mark.asyncio
    async def test_empty_scope_returns_empty_list(
        self, rsa_keys: tuple[rsa.RSAPrivateKey, rsa.RSAPublicKey]
    ) -> None:
        """Token without scope claim produces empty scopes list."""
        private_key, public_key = rsa_keys
        jwks = _build_jwks((public_key, "key-1", "RS256"))

        # Build token without scope claim.
        claims: dict[str, Any] = {
            "iss": ISSUER,
            "aud": AUDIENCE,
            "exp": int(time.time()) + 3600,
            "azp": "client",
        }
        token = pyjwt.encode(
            claims, private_key, algorithm="RS256", headers={"kid": "key-1"}
        )

        with respx.mock:
            respx.get(JWKS_URL).respond(json=jwks)

            verifier = JwtTokenVerifier(
                jwks_url=JWKS_URL, issuer=ISSUER, audience=AUDIENCE
            )
            try:
                result = await verifier.verify_token(token)
                assert result is not None
                assert result.scopes == []
            finally:
                await verifier.close()


# ---------------------------------------------------------------------------
# 5. JWKS endpoint failures
# ---------------------------------------------------------------------------


class TestJwksEndpointFailures:
    """Test behaviour when the JWKS endpoint is unavailable."""

    @pytest.mark.asyncio
    async def test_jwks_500_rejects_token(
        self, rsa_keys: tuple[rsa.RSAPrivateKey, rsa.RSAPublicKey]
    ) -> None:
        """When JWKS returns 500, token verification fails gracefully."""
        private_key, _ = rsa_keys
        token = _sign_token(private_key, "key-1")

        with respx.mock:
            respx.get(JWKS_URL).respond(status_code=500)

            verifier = JwtTokenVerifier(
                jwks_url=JWKS_URL, issuer=ISSUER, audience=AUDIENCE
            )
            try:
                result = await verifier.verify_token(token)
                assert result is None
            finally:
                await verifier.close()

    @pytest.mark.asyncio
    async def test_jwks_timeout_rejects_token(
        self, rsa_keys: tuple[rsa.RSAPrivateKey, rsa.RSAPublicKey]
    ) -> None:
        """When JWKS times out, token verification fails gracefully."""
        import httpx

        private_key, _ = rsa_keys
        token = _sign_token(private_key, "key-1")

        with respx.mock:
            respx.get(JWKS_URL).mock(
                side_effect=httpx.TimeoutException("connection timed out")
            )

            verifier = JwtTokenVerifier(
                jwks_url=JWKS_URL, issuer=ISSUER, audience=AUDIENCE
            )
            try:
                result = await verifier.verify_token(token)
                assert result is None
            finally:
                await verifier.close()


# ---------------------------------------------------------------------------
# 6. create_auth_context integration
# ---------------------------------------------------------------------------


class TestCreateAuthContextIntegration:
    """Test that create_auth_context() correctly wires JwtTokenVerifier."""

    def test_jwt_auth_context_creates_verifier(self) -> None:
        """create_auth_context with auth_type=jwt creates a JwtTokenVerifier."""
        jwt_config = JwtConfig(
            jwks_url=JWKS_URL,
            issuer=ISSUER,
            audience=AUDIENCE,
        )

        ctx: AuthContext = create_auth_context(
            auth_server_url=ISSUER,
            server_url=AUDIENCE,
            auth_type="jwt",
            jwt_config=jwt_config,
        )

        assert isinstance(ctx.token_verifier, JwtTokenVerifier)
        assert str(ctx.auth_settings.issuer_url).rstrip("/") == ISSUER
        assert str(ctx.auth_settings.resource_server_url).rstrip("/") == AUDIENCE

    @pytest.mark.asyncio
    async def test_jwt_context_verifies_token(
        self, rsa_keys: tuple[rsa.RSAPrivateKey, rsa.RSAPublicKey]
    ) -> None:
        """Full flow: create_auth_context → sign token → verify."""
        private_key, public_key = rsa_keys
        jwks = _build_jwks((public_key, "ctx-key-1", "RS256"))
        token = _sign_token(private_key, "ctx-key-1")

        jwt_config = JwtConfig(
            jwks_url=JWKS_URL,
            issuer=ISSUER,
            audience=AUDIENCE,
        )

        ctx = create_auth_context(
            auth_server_url=ISSUER,
            server_url=AUDIENCE,
            auth_type="jwt",
            jwt_config=jwt_config,
        )

        with respx.mock:
            respx.get(JWKS_URL).respond(json=jwks)

            try:
                result = await ctx.token_verifier.verify_token(token)
                assert result is not None
                assert result.client_id == "test-client"
                assert "mcp:postgres:access" in result.scopes
            finally:
                if hasattr(ctx.token_verifier, "close"):
                    await ctx.token_verifier.close()  # type: ignore[attr-defined]

    def test_introspection_auth_context_creates_different_verifier(self) -> None:
        """create_auth_context with auth_type=introspection creates
        IntrospectionTokenVerifier."""
        from pg_airman_mcp.token_verifier import IntrospectionTokenVerifier

        ctx = create_auth_context(
            auth_server_url=ISSUER,
            server_url=AUDIENCE,
            auth_type="introspection",
        )

        assert isinstance(ctx.token_verifier, IntrospectionTokenVerifier)

    def test_switching_auth_type_produces_correct_verifier(self) -> None:
        """Switching auth_type between jwt and introspection creates
        the correct verifier type each time."""
        from pg_airman_mcp.token_verifier import IntrospectionTokenVerifier

        jwt_config = JwtConfig(jwks_url=JWKS_URL, issuer=ISSUER, audience=AUDIENCE)

        jwt_ctx = create_auth_context(
            auth_server_url=ISSUER,
            server_url=AUDIENCE,
            auth_type="jwt",
            jwt_config=jwt_config,
        )
        intro_ctx = create_auth_context(
            auth_server_url=ISSUER,
            server_url=AUDIENCE,
            auth_type="introspection",
        )

        assert isinstance(jwt_ctx.token_verifier, JwtTokenVerifier)
        assert isinstance(intro_ctx.token_verifier, IntrospectionTokenVerifier)


# ---------------------------------------------------------------------------
# 7. Multiple algorithms
# ---------------------------------------------------------------------------


class TestMultipleAlgorithms:
    """Test verifier configured to accept multiple algorithm families."""

    @pytest.mark.asyncio
    async def test_rsa_and_ec_tokens_both_accepted(self) -> None:
        """Verifier accepting RS256+ES256 validates both token types."""
        rsa_priv, rsa_pub = _rsa_keypair()
        ec_priv, ec_pub = _ec_keypair()

        jwks = _build_jwks(
            (rsa_pub, "rsa-key", "RS256"),
            (ec_pub, "ec-key", "ES256"),
        )

        with respx.mock:
            respx.get(JWKS_URL).respond(json=jwks)

            verifier = JwtTokenVerifier(
                jwks_url=JWKS_URL,
                issuer=ISSUER,
                audience=AUDIENCE,
                algorithms=["RS256", "ES256"],
            )
            try:
                # RSA token
                rsa_token = _sign_token(rsa_priv, "rsa-key", algorithm="RS256")
                rsa_result = await verifier.verify_token(rsa_token)
                assert rsa_result is not None

                # EC token — need to refresh because cache may be stale.
                verifier._jwks_last_refresh = 0.0
                ec_token = _sign_token(ec_priv, "ec-key", algorithm="ES256")
                ec_result = await verifier.verify_token(ec_token)
                assert ec_result is not None
            finally:
                await verifier.close()
