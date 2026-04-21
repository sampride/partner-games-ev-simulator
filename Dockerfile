# Use the official Python 3.13 slim image
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_SYSTEM_PYTHON=1 \
    PYTHONPATH=/app/src

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock ./

# Install dependencies
RUN uv sync --no-dev

COPY src/ ./src/

# IMPORTANT: use uv to run the app
CMD ["uv", "run", "python", "-m", "simulator.main"]