FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install deps in a separate layer for cache efficiency
COPY pyproject.toml /app/pyproject.toml
RUN pip install --no-cache-dir -e .

# Copy source last
COPY src /app/src
COPY db /app/db

# Default port
EXPOSE 8020

# Health check — the FastAPI /health endpoint
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8020/health', timeout=3)" || exit 1

CMD ["python", "-m", "side_stream.main"]
