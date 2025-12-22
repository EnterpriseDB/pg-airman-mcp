# First, build the application in the `/app` directory.
# See `Dockerfile` for details.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# Disable Python downloads, because we want to use the system interpreter
# across both images. If using a managed Python version, it needs to be
# copied from the build image into the final image; see `standalone.Dockerfile`
# for an example.
ENV UV_PYTHON_DOWNLOADS=0

WORKDIR /app
RUN apt-get update \
  && apt-get install -y --no-install-recommends gcc libpq-dev python3-dev \
  && rm -rf /var/lib/apt/lists/*
RUN --mount=type=cache,target=/root/.cache/uv \
  --mount=type=bind,source=uv.lock,target=uv.lock \
  --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
  uv sync --frozen --no-install-project --no-dev
ADD . /app
RUN --mount=type=cache,target=/root/.cache/uv \
  uv sync --frozen --no-dev


FROM python:3.12-slim-bookworm
# It is important to use the image that matches the builder, as the path to the
# Python executable must be the same, e.g., using `python:3.11-slim-bookworm`
# will fail.

# Install runtime system dependencies, upgrade pip to latest version
# and create non-root user.
RUN apt-get update && apt-get install -y --no-install-recommends \
  dnsutils \
  iputils-ping \
  libpq-dev \
  net-tools \
  && rm -rf /var/lib/apt/lists/* \
  && pip install --no-cache-dir --upgrade pip \
  && groupadd -r app --gid=1000 \
  && useradd -r -g app --uid=1000 --home-dir=/app --shell=/bin/bash app \
  && mkdir -p /app \
  && chown -R app:app /app

# Clean up unnecessary packages installed by dependencies
RUN apt-get purge -y libldap-2.5-0 libsqlite3-0 \
  && apt-get autoremove -y

COPY --from=builder --chown=app:app /app /app

ENV PATH="/app/.venv/bin:$PATH"

ARG TARGETPLATFORM
ARG BUILDPLATFORM
LABEL org.opencontainers.image.description="Pg Airman MCP Agent - Multi-architecture container (${TARGETPLATFORM})"
LABEL org.opencontainers.image.source="https://github.com/EnterpriseDB/pg-airman-mcp"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.vendor="EnterpriseDB"
LABEL org.opencontainers.image.url="https://www.enterprisedb.com"

COPY --chown=app:app --chmod=755 docker-entrypoint.sh /app/

# Switch to non-root user
USER app
WORKDIR /app

# Expose the SSE port
EXPOSE 8000

# Run the pg-airman-mcp server
# Users can pass a database URI or individual connection arguments:
#   docker run -it --rm pg-airman-mcp postgres://user:pass@host:port/dbname
#   docker run -it --rm pg-airman-mcp -h myhost -p 5432 -U myuser -d mydb
ENTRYPOINT ["/app/docker-entrypoint.sh", "pg-airman-mcp"]
CMD []
