# Dagster service image for the batch-vs-stream reconciliation.
#
# Built from the REPO ROOT context (see docker-compose.yml `dagster.build`) so the
# generator + reconciliation packages and the integration dependency group
# (dagster, dagster-webserver, psycopg2, clickhouse-driver) are installed.
#
# The container runs `dagster dev`, which serves the UI on :3000 AND starts the
# daemon (sensors/schedules). The reconciliation assets/check are loaded from
# reconciliation.assets:defs. Materialise them from the UI, or run the
# in-process integration test (tests/integration/test_reconciliation.py) for an
# automated, daemon-free verification path (ADR-0005).
FROM ghcr.io/astral-sh/uv:python3.13-trixie-slim

WORKDIR /opt/dagster/app

ENV UV_LINK_MODE=copy \
    DAGSTER_HOME=/opt/dagster/home

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --group integration

COPY generator ./generator
COPY reconciliation ./reconciliation
COPY dagster_service/workspace.yaml ./workspace.yaml

# Install the local generator + reconciliation packages into the venv so
# `dagster dev` can import reconciliation.assets regardless of working directory.
RUN uv pip install -e .

RUN mkdir -p "$DAGSTER_HOME"

EXPOSE 3000

# `dagster dev` binds the webserver to 0.0.0.0 so the published :3000 is reachable
# from the host, and starts the daemon in the same process for the demo.
CMD ["uv", "run", "dagster", "dev", "-w", "workspace.yaml", "--host", "0.0.0.0", "--port", "3000"]
