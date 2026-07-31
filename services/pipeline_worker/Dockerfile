FROM python:3.11-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY services ./services

RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --upgrade pip \
    && /opt/venv/bin/pip install .


FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH" \
    WORKER_HEALTH_PORT=8090

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
COPY pyproject.toml README.md ./
COPY services ./services

EXPOSE 8090

CMD ["python", "-m", "wealthsignal_pipeline.worker_health"]
