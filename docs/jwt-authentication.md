# JWT Authentication Guide

This guide covers JWT token verification for Pg Airman MCP, including when to use it, how to configure it, and provider-specific setup instructions.

## Choosing Between Introspection and JWT

Pg Airman MCP supports two token verification methods. Choose based on your requirements:

| Consideration | Introspection | JWT |
|---|---|---|
| **Validation approach** | Remote call to authorization server per request | Local signature verification using cached public keys |
| **Latency** | Higher (network round-trip per token) | Lower (no per-request network call) |
| **Token revocation** | Immediate — server checks real-time status | Delayed — tokens valid until expiry |
| **Network dependency** | Requires auth server reachable for every request | Only needs JWKS endpoint reachable periodically |
| **Best for** | Environments needing instant revocation | High-throughput or latency-sensitive deployments |

**Recommendation**: Use JWT when your authorization server issues short-lived tokens (5–15 minutes) and you want lower latency. Use introspection when you need real-time revocation or are working with opaque (non-JWT) tokens.

## Quick Start

### Minimal JWT Configuration

```bash
export AIRMAN_MCP_AUTH_ENABLED=true
export AIRMAN_MCP_AUTH_TYPE=jwt
export AIRMAN_MCP_AUTH_SERVER_URL=https://auth.example.com
export AIRMAN_MCP_JWT_ISSUER=https://auth.example.com

pg-airman-mcp "postgresql://..." --access-mode=restricted
```

The JWKS URL defaults to `{auth-server-url}/.well-known/jwks.json` and the audience defaults to the server URL. These defaults work with most identity providers.

### CLI Arguments

All JWT settings are also available as CLI arguments:

```bash
pg-airman-mcp "postgresql://..." \
  --auth-enabled \
  --auth-type jwt \
  --auth-server-url https://auth.example.com \
  --jwt-issuer https://auth.example.com \
  --jwt-audience https://mcp.example.com \
  --jwt-algorithms RS256 \
  --jwt-clock-skew 30 \
  --jwt-jwks-cache-ttl 300
```

## Configuration Reference

| Variable | CLI Argument | Description | Default |
|---|---|---|---|
| `AIRMAN_MCP_AUTH_TYPE` | `--auth-type` | Set to `jwt` for JWT verification | `introspection` |
| `AIRMAN_MCP_JWT_ISSUER` | `--jwt-issuer` | Expected `iss` claim (required) | *(none)* |
| `AIRMAN_MCP_JWT_JWKS_URL` | `--jwt-jwks-url` | JWKS endpoint URL | `{auth-server-url}/.well-known/jwks.json` |
| `AIRMAN_MCP_JWT_AUDIENCE` | `--jwt-audience` | Expected `aud` claim | *(server URL)* |
| `AIRMAN_MCP_JWT_ALGORITHMS` | `--jwt-algorithms` | Comma-separated signing algorithms | `RS256` |
| `AIRMAN_MCP_JWT_CLOCK_SKEW` | `--jwt-clock-skew` | Clock skew tolerance (seconds) | `30` |
| `AIRMAN_MCP_JWT_JWKS_CACHE_TTL` | `--jwt-jwks-cache-ttl` | JWKS cache TTL (seconds) | `300` |

## Provider-Specific Examples

### Keycloak

Keycloak exposes a standard JWKS endpoint per realm.

```bash
AIRMAN_MCP_AUTH_ENABLED=true
AIRMAN_MCP_AUTH_TYPE=jwt
AIRMAN_MCP_AUTH_SERVER_URL=https://keycloak.example.com/realms/myrealm
AIRMAN_MCP_JWT_ISSUER=https://keycloak.example.com/realms/myrealm
AIRMAN_MCP_JWT_JWKS_URL=https://keycloak.example.com/realms/myrealm/protocol/openid-connect/certs
```

**JWKS URL pattern**: `https://<host>/realms/<realm>/protocol/openid-connect/certs`

> **Note**: Keycloak's JWKS endpoint path does not follow the `.well-known/jwks.json` convention, so you must set `AIRMAN_MCP_JWT_JWKS_URL` explicitly.

### Auth0

Auth0 uses a tenant-based URL structure.

```bash
AIRMAN_MCP_AUTH_ENABLED=true
AIRMAN_MCP_AUTH_TYPE=jwt
AIRMAN_MCP_AUTH_SERVER_URL=https://mytenant.auth0.com
AIRMAN_MCP_JWT_ISSUER=https://mytenant.auth0.com/
AIRMAN_MCP_JWT_AUDIENCE=https://mcp.example.com
```

**JWKS URL pattern**: `https://<tenant>.auth0.com/.well-known/jwks.json` (default works)

> **Note**: Auth0 issuers include a trailing slash. Ensure `AIRMAN_MCP_JWT_ISSUER` matches exactly.

### Microsoft Entra ID (Azure AD)

Entra ID uses a tenant-specific discovery endpoint.

```bash
AIRMAN_MCP_AUTH_ENABLED=true
AIRMAN_MCP_AUTH_TYPE=jwt
AIRMAN_MCP_AUTH_SERVER_URL=https://login.microsoftonline.com/<tenant-id>/v2.0
AIRMAN_MCP_JWT_ISSUER=https://login.microsoftonline.com/<tenant-id>/v2.0
AIRMAN_MCP_JWT_JWKS_URL=https://login.microsoftonline.com/<tenant-id>/discovery/v2.0/keys
AIRMAN_MCP_JWT_AUDIENCE=<application-client-id>
```

**JWKS URL pattern**: `https://login.microsoftonline.com/<tenant-id>/discovery/v2.0/keys`

> **Note**: The audience for Entra ID is typically the Application (client) ID, not a URL.

## Security Considerations

### Resource Validation vs Audience

The `--auth-validate-resource` flag (RFC 8707 resource validation) is **not compatible** with JWT authentication. If you attempt to use both, the server will reject the configuration at startup:

```text
ValueError: --auth-validate-resource cannot be used with --auth-type=jwt.
JWT uses audience validation instead (--jwt-audience).
```

**Why?** In introspection mode, resource validation checks that the token was issued for a specific resource server. In JWT mode, the `aud` (audience) claim serves the same purpose — it identifies the intended recipient of the token. Use `--jwt-audience` to specify the expected audience.

### Algorithm Restrictions

Only asymmetric signing algorithms are accepted:

| Allowed | Forbidden |
|---|---|
| RS256, RS384, RS512 | HS256, HS384, HS512 |
| ES256, ES384, ES512 | `none` |

Symmetric algorithms (`HS*`) are rejected because they use a shared secret — accepting them would mean any party with the secret could forge tokens. The `none` algorithm is rejected because it disables signature verification entirely.

The default algorithm is `RS256`, which is supported by all major identity providers. Use `ES256` for smaller tokens and faster verification with elliptic curve keys.

### JWKS Caching and Key Rotation

The verifier caches JWKS public keys to avoid fetching them on every request:

- **Cache TTL**: Keys are refreshed after the TTL expires (default: 300 seconds / 5 minutes).
- **Unknown key ID**: If a token presents a `kid` not in the cache, an immediate refresh is triggered (rate-limited to once every 30 seconds).
- **Key rotation**: When your identity provider rotates keys, tokens signed with the new key trigger a JWKS refresh. The new key is picked up automatically.

**Recommended token expiry**: 5–15 minutes. Short-lived tokens reduce the window of exposure if a token is compromised, compensating for the lack of real-time revocation in JWT mode.

### SSRF Prevention

The JWKS URL is validated at startup:

- **HTTPS**: Always allowed.
- **HTTP**: Only allowed for `localhost`, `127.0.0.1`, or `::1` (development use).
- **Other schemes**: Rejected.

This prevents an attacker from pointing the JWKS URL at internal services.

### Token Claims

The verifier extracts the following from validated tokens:

| Field | JWT Claims (priority order) |
|---|---|
| `client_id` | `azp` → `client_id` → `sub` → `"unknown"` |
| `scopes` | Space-delimited `scope` claim (RFC 9068) |
| `expires_at` | `exp` claim |

## Docker Configuration

### JWT with Docker (stdio)

```json
{
  "mcpServers": {
    "postgres": {
      "command": "docker",
      "args": [
        "run", "-i", "--rm",
        "-e", "AIRMAN_MCP_DATABASE_URL",
        "-e", "AIRMAN_MCP_AUTH_ENABLED",
        "-e", "AIRMAN_MCP_AUTH_TYPE",
        "-e", "AIRMAN_MCP_AUTH_SERVER_URL",
        "-e", "AIRMAN_MCP_JWT_ISSUER",
        "-e", "AIRMAN_MCP_SERVER_URL",
        "enterprisedb/pg-airman-mcp",
        "--access-mode=restricted"
      ],
      "env": {
        "AIRMAN_MCP_DATABASE_URL": "postgresql://user:pass@localhost:5432/dbname",
        "AIRMAN_MCP_AUTH_ENABLED": "true",
        "AIRMAN_MCP_AUTH_TYPE": "jwt",
        "AIRMAN_MCP_AUTH_SERVER_URL": "https://auth.example.com",
        "AIRMAN_MCP_JWT_ISSUER": "https://auth.example.com",
        "AIRMAN_MCP_SERVER_URL": "http://localhost:8000"
      }
    }
  }
}
```

### JWT with Docker (SSE)

```bash
docker run -p 8000:8000 \
  -e AIRMAN_MCP_DATABASE_URL=postgresql://user:pass@localhost:5432/dbname \
  -e AIRMAN_MCP_AUTH_ENABLED=true \
  -e AIRMAN_MCP_AUTH_TYPE=jwt \
  -e AIRMAN_MCP_AUTH_SERVER_URL=https://auth.example.com \
  -e AIRMAN_MCP_JWT_ISSUER=https://auth.example.com \
  enterprisedb/pg-airman-mcp --access-mode=restricted --transport=sse
```

## Migrating from Introspection to JWT

If you are currently using token introspection and want to switch to JWT:

1. **Verify your authorization server issues JWTs**: Introspection works with any token format, but JWT mode requires actual JWT access tokens signed with a supported algorithm.

2. **Find your JWKS endpoint**: Most providers expose it at a well-known URL. Check your provider's OpenID Connect discovery endpoint (`/.well-known/openid-configuration`) — it contains a `jwks_uri` field.

3. **Update configuration**:
   ```bash
   # Remove introspection-specific settings
   # unset AIRMAN_MCP_AUTH_INTROSPECTION_ENDPOINT
   # unset AIRMAN_MCP_AUTH_INTROSPECTION_CLIENT_ID
   # unset AIRMAN_MCP_AUTH_INTROSPECTION_CLIENT_SECRET

   # Add JWT settings
   export AIRMAN_MCP_AUTH_TYPE=jwt
   export AIRMAN_MCP_JWT_ISSUER=https://auth.example.com
   # JWKS URL defaults to {auth-server-url}/.well-known/jwks.json
   ```

4. **Test**: Restart the server and verify that existing tokens are accepted. Check logs for `Authentication ENABLED` with `Auth type: jwt`.

> **Note**: You cannot use both introspection and JWT simultaneously. Each server instance uses one verification method. If you need to support both token types during a migration, run two server instances temporarily.

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| `401` on all requests | Wrong issuer or audience | Check `AIRMAN_MCP_JWT_ISSUER` matches the `iss` claim in your tokens exactly |
| `JWT algorithm 'X' not in allowed list` in logs | Token signed with unexpected algorithm | Add the algorithm to `AIRMAN_MCP_JWT_ALGORITHMS` |
| `No matching JWKS key for kid=X` | JWKS endpoint not serving the expected key | Verify `AIRMAN_MCP_JWT_JWKS_URL` is correct and accessible |
| `JWKS URL fails SSRF validation` | HTTP URL pointing to non-localhost host | Use HTTPS or set URL to localhost for development |
| `--jwt-issuer is required` | Missing issuer configuration | Set `AIRMAN_MCP_JWT_ISSUER` |
