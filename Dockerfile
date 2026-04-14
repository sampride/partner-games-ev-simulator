# Use the official Python 3.13 slim image
FROM python:3.13-slim

# Set environment variables for Python and uv
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_SYSTEM_PYTHON=1

# Install uv directly
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Copy dependency files and install
COPY pyproject.toml uv.lock ./
RUN uv sync --no-dev

# Copy the application source code
COPY src/ ./src/

# We do NOT copy config/ or data/ because they will be volume-mounted at runtime

# Run the engine
CMD ["python", "src/simulator/main.py"]