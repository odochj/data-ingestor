FROM python:3.12-slim

WORKDIR /app

# Use 'uv' to manage dependencies and virtual environments consistently
# Copy project metadata and lockfile first to leverage Docker layer caching
COPY pyproject.toml uv.lock ./

# Install uv and build essentials
RUN apt-get update && apt-get install -y --no-install-recommends curl build-essential gcc libpq-dev && \
    pip install --no-cache-dir uv setuptools wheel && \
    rm -rf /var/lib/apt/lists/*

# Install pinned dependencies via uv and install the project in editable mode
# uv sync will install the locked resolver; uv pip installs into the uv-managed venv
RUN uv sync && uv pip install --no-cache-dir -e .

# Copy the rest of the source
COPY . .

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s CMD curl -f http://localhost:8000/health || exit 1

# Use uv run so commands execute within the uv-managed environment
CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
