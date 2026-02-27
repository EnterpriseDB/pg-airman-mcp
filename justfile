#!/usr/bin/env just --justfile
set shell := ["zsh", "-cu"]
set fallback

# Default docker image name
docker_image := "pg-airman-mcp"

# Default: list all recipes
default:
  just -u --list

# ── Development ──────────────────────────────────────────────────────

# Install/sync project dependencies
sync:
  uv sync

# Run the MCP server with the Inspector (dev mode)
dev:
  uv run mcp dev -e . src/pg_airman_mcp/server.py

# Run the MCP server via CLI (requires database_url)
run database_url *args:
  uv run pg-airman-mcp "{{database_url}}" {{args}}

# ── Code Quality ─────────────────────────────────────────────────────

# Auto-format code with ruff
format:
  uv run ruff format .

# Run ruff linter with auto-fix
lint:
  uv run ruff check --fix .

# Run pyright type checker
typecheck:
  uv run pyright

# Run all code quality checks (CI-equivalent, no auto-fix)
check:
  uv run ruff format --check .
  uv run ruff check .
  uv run pyright

# ── Testing ──────────────────────────────────────────────────────────

# Run all tests
test *args:
  uv run pytest {{args}}

# Run unit tests only
test-unit *args:
  uv run pytest tests/unit/ {{args}}

# Run integration tests only (requires Docker)
test-integration *args:
  uv run pytest tests/integration/ {{args}}

# Run tests with full coverage reporting (CI-equivalent)
test-coverage:
  uv run pytest -v --log-cli-level=INFO --cov-report=xml:coverage.xml --junitxml=pytest.xml --cov src/pg_airman_mcp/ --cov-report term tests/

# ── Docker ───────────────────────────────────────────────────────────

# Build the Docker image
docker-build tag="latest":
  docker build -t {{docker_image}}:{{tag}} .

# Run the Docker container (pass database URL and optional args)
docker-run database_url tag="latest" *args:
  docker run -it --rm -p 8000:8000 {{docker_image}}:{{tag}} "{{database_url}}" {{args}}

# ── Release ──────────────────────────────────────────────────────────

# Print release workflow instructions
release-help:
  @echo "- update version in pyproject.toml"
  @echo "- uv sync"
  @echo "- git commit"
  @echo "- git push && merge to main"
  @echo '- just release 0.0.0 "note"'
  @echo 'OR'
  @echo '- just prerelease 0.0.0 1 "note"'

# Create a release (version format: 0.0.0, no 'v' prefix)
release version note extra="":
  #!/usr/bin/env bash
  if [[ "{{version}}" == v* ]]; then
    echo "Error: Do not include 'v' prefix in version. It will be added automatically."
    exit 1
  fi
  uv build && git tag -a "v{{version}}" -m "Release v{{version}}" || true && git push --tags && gh release create "v{{version}}" --title "PG Airman MCP v{{version}}" --notes "{{note}}" {{extra}} dist/*.whl dist/*.tar.gz

# Create a pre-release (version format: 0.0.0, rc number, note)
prerelease version rc note:
  just release "{{version}}rc{{rc}}" "{{note}}" "--prerelease"

# ── Misc ─────────────────────────────────────────────────────────────

# Launch Claude Desktop via Nix (Linux)
nix-claude-desktop:
  NIXPKGS_ALLOW_UNFREE=1 nix run "github:k3d3/claude-desktop-linux-flake#claude-desktop-with-fhs" --impure
