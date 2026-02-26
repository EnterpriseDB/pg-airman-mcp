"""Unit tests for IntrospectionTokenVerifier SSRF prevention."""
# pyright: reportPrivateUsage=false

from unittest.mock import AsyncMock, MagicMock

import pytest

from pg_airman_mcp.token_verifier import IntrospectionTokenVerifier


class TestSSRFPrevention:
    """Test that the SSRF guard rejects unsafe introspection endpoints."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "endpoint",
        [
            "http://localhost.evil.com/introspect",
            "http://localhost.evil.com:9000/introspect",
            "http://127.0.0.1.nip.io/introspect",
            "http://evil.com/introspect",
            "ftp://localhost/introspect",
            "http://10.0.0.1/introspect",
            "http://192.168.1.1/introspect",
        ],
    )
    async def test_rejects_unsafe_endpoints(self, endpoint: str) -> None:
        """Unsafe hostnames or schemes are rejected."""
        verifier = IntrospectionTokenVerifier(
            introspection_endpoint=endpoint,
            server_url="http://localhost:8000",
        )
        result = await verifier.verify_token("fake_token")
        assert result is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "endpoint",
        [
            "https://auth.example.com/introspect",
            "https://any-host:8443/introspect",
            "http://localhost/introspect",
            "http://localhost:9000/introspect",
            "http://127.0.0.1/introspect",
            "http://127.0.0.1:9000/introspect",
            "http://[::1]/introspect",
            "http://[::1]:9000/introspect",
        ],
    )
    async def test_allows_safe_endpoints(self, endpoint: str) -> None:
        """HTTPS and exact localhost/127.0.0.1/::1 pass the guard."""
        verifier = IntrospectionTokenVerifier(
            introspection_endpoint=endpoint,
            server_url="http://localhost:8000",
        )

        # httpx.Response.json() is sync, so use MagicMock for response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "active": True,
            "client_id": "test",
            "scope": "read",
        }

        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client.is_closed = False
        verifier._client = mock_client

        result = await verifier.verify_token("fake_token")
        # Request was made (not blocked by SSRF guard)
        mock_client.post.assert_called_once()
        assert result is not None


class TestClientLifecycle:
    """Test that the shared HTTP client is managed correctly."""

    def test_client_created_lazily(self) -> None:
        """Client is None until first use."""
        verifier = IntrospectionTokenVerifier(
            introspection_endpoint="http://localhost:9000/introspect",
            server_url="http://localhost:8000",
        )
        assert verifier._client is None

    def test_get_client_returns_client(self) -> None:
        """_get_client() creates and returns an httpx.AsyncClient."""
        import httpx

        verifier = IntrospectionTokenVerifier(
            introspection_endpoint="http://localhost:9000/introspect",
            server_url="http://localhost:8000",
        )
        client = verifier._get_client()
        assert isinstance(client, httpx.AsyncClient)
        assert not client.is_closed

    def test_get_client_reuses_existing(self) -> None:
        """_get_client() returns the same instance on subsequent calls."""
        verifier = IntrospectionTokenVerifier(
            introspection_endpoint="http://localhost:9000/introspect",
            server_url="http://localhost:8000",
        )
        client1 = verifier._get_client()
        client2 = verifier._get_client()
        assert client1 is client2

    @pytest.mark.asyncio
    async def test_close_shuts_down_client(self) -> None:
        """close() calls aclose() on the underlying client."""
        verifier = IntrospectionTokenVerifier(
            introspection_endpoint="http://localhost:9000/introspect",
            server_url="http://localhost:8000",
        )
        mock_client = AsyncMock()
        mock_client.is_closed = False
        verifier._client = mock_client

        await verifier.close()
        mock_client.aclose.assert_awaited_once()
        assert verifier._client is None

    @pytest.mark.asyncio
    async def test_close_noop_when_no_client(self) -> None:
        """close() is safe to call when no client was ever created."""
        verifier = IntrospectionTokenVerifier(
            introspection_endpoint="http://localhost:9000/introspect",
            server_url="http://localhost:8000",
        )
        await verifier.close()  # Should not raise


class TestClientCredentials:
    """Test that introspection client credentials are handled correctly."""

    def test_constructor_defaults_to_none(self) -> None:
        """Client credentials default to None when not provided."""
        verifier = IntrospectionTokenVerifier(
            introspection_endpoint="http://localhost:9000/introspect",
            server_url="http://localhost:8000",
        )
        assert verifier.client_id is None
        assert verifier.client_secret is None

    def test_constructor_stores_credentials(self) -> None:
        """Client credentials are stored when provided."""
        verifier = IntrospectionTokenVerifier(
            introspection_endpoint="http://localhost:9000/introspect",
            server_url="http://localhost:8000",
            client_id="my-client",
            client_secret="my-secret",
        )
        assert verifier.client_id == "my-client"
        assert verifier.client_secret == "my-secret"

    @pytest.mark.asyncio
    async def test_credentials_included_in_post_body(self) -> None:
        """When both client_id and client_secret are set, they are sent in POST."""
        verifier = IntrospectionTokenVerifier(
            introspection_endpoint="http://localhost:9000/introspect",
            server_url="http://localhost:8000",
            client_id="my-client",
            client_secret="my-secret",
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "active": True,
            "client_id": "test",
            "scope": "read",
        }

        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client.is_closed = False
        verifier._client = mock_client

        await verifier.verify_token("test-token")

        mock_client.post.assert_called_once()
        call_kwargs = mock_client.post.call_args
        post_data: dict[str, str] = call_kwargs.kwargs["data"]
        assert post_data["token"] == "test-token"
        assert post_data["client_id"] == "my-client"
        assert post_data["client_secret"] == "my-secret"

    @pytest.mark.asyncio
    async def test_credentials_omitted_when_none(self) -> None:
        """When credentials are None, only token is sent in POST."""
        verifier = IntrospectionTokenVerifier(
            introspection_endpoint="http://localhost:9000/introspect",
            server_url="http://localhost:8000",
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "active": True,
            "client_id": "test",
            "scope": "read",
        }

        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client.is_closed = False
        verifier._client = mock_client

        await verifier.verify_token("test-token")

        call_kwargs = mock_client.post.call_args
        post_data: dict[str, str] = call_kwargs.kwargs["data"]
        assert post_data == {"token": "test-token"}
        assert "client_id" not in post_data
        assert "client_secret" not in post_data

    @pytest.mark.asyncio
    async def test_credentials_omitted_when_partial(self) -> None:
        """When only one of client_id/client_secret is set, neither is sent."""
        verifier = IntrospectionTokenVerifier(
            introspection_endpoint="http://localhost:9000/introspect",
            server_url="http://localhost:8000",
            client_id="my-client",
            # client_secret intentionally omitted
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "active": True,
            "client_id": "test",
            "scope": "read",
        }

        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client.is_closed = False
        verifier._client = mock_client

        await verifier.verify_token("test-token")

        call_kwargs = mock_client.post.call_args
        post_data: dict[str, str] = call_kwargs.kwargs["data"]
        assert post_data == {"token": "test-token"}
        assert "client_id" not in post_data
