# syntax=docker/dockerfile:1

# uv-provided base image bundles Python 3.13 + uv (matches .python-version
# and pyproject `requires-python = >=3.13`). Dagster's GraphQL/graphene stack
# does not run on 3.14 (coroutines never awaited → leaked DB connections).
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

# Install into the system environment so the dagster/dagster-webserver/
# dagster-daemon console scripts land on PATH (no `uv run` needed at runtime).
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/usr/local \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Layer 1 — dependencies only. The local i14y-client is an editable path
# dependency (see [tool.uv.sources]) so its sources must be present already.
COPY pyproject.toml uv.lock README.md ./
COPY packages ./packages
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

# Layer 2 — application code + editable install of the root project.
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# Dagster instance config (Postgres storage, queued runs, concurrency pool).
ENV DAGSTER_HOME=/opt/dagster/dagster_home
COPY deploy/dagster.yaml $DAGSTER_HOME/dagster.yaml
COPY deploy/workspace.yaml /opt/dagster/workspace.yaml

# Default command runs the code gRPC server; webserver/daemon override it
# in docker-compose.
EXPOSE 4000
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", \
     "-m", "metadataswiss_connector.dagster_defs"]
