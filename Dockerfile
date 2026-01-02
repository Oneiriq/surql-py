# Stage: Base
FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_SYSTEM_PYTHON=1

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN groupadd -r appuser && \
    useradd -r -g appuser -u 1000 -m -s /bin/bash appuser

WORKDIR /app


# Stage: Build
FROM base AS builder

ARG UV_VERSION=0.5.11
RUN curl -LsSf https://astral.sh/uv/${UV_VERSION}/install.sh | sh && \
    mv /root/.local/bin/uv /usr/local/bin/ && \
    mv /root/.local/bin/uvx /usr/local/bin/

COPY pyproject.toml .python-version README.md ./
COPY src/ ./src/

RUN uv venv /app/.venv && \
    . /app/.venv/bin/activate && \
    uv pip install -e .


# Stage: Production (Runtime)
FROM base AS production

COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src /app/src

ENV PATH="/app/.venv/bin:$PATH" \
    ENVIRONMENT=production \
    DEBUG=false \
    LOG_LEVEL=INFO

RUN chown -R appuser:appuser /app

USER appuser

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

CMD ["python", "-m", "python_uv_simple"]


# Stage: Development (Runtime)
FROM builder AS development

RUN . /app/.venv/bin/activate && \
    uv pip install -e ".[dev]"

COPY tests/ ./tests/

ENV PATH="/app/.venv/bin:$PATH" \
    ENVIRONMENT=development \
    DEBUG=true \
    LOG_LEVEL=DEBUG

RUN chown -R appuser:appuser /app

USER appuser

CMD ["pytest-watch", "--", "-v"]
