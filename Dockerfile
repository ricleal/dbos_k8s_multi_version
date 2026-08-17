FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PYTHONUNBUFFERED=1

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY main.py ./
COPY poc ./poc

# Exec form, no shell wrapper: the interpreter is PID 1 and receives SIGTERM directly.
ENTRYPOINT ["/app/.venv/bin/python", "main.py"]
