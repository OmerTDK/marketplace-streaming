FROM ghcr.io/astral-sh/uv:python3.13-trixie-slim

WORKDIR /app

ENV UV_LINK_MODE=copy

COPY pyproject.toml ./
RUN uv sync

COPY . .

CMD ["uv", "run", "pytest", "-v"]
