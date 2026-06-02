# Build context: monorepo root (docker build -f schism/services/ingestion/Dockerfile .)
FROM python:3.11-slim

WORKDIR /app

# shared package — installed from COPY in production.
# In local dev, docker-compose.override.yml mounts ./schism/shared:/app/shared
# over this layer, enabling hot reload without rebuild.
COPY schism/shared /app/shared
RUN pip install --no-cache-dir /app/shared

# Service dependencies (separate layer — rebuilds only when requirements change).
COPY schism/services/ingestion/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Source
COPY schism/services/ingestion/src /app/src

ENV PYTHONPATH=/app/src

# data/raw is always a host volume — never baked into the image.
VOLUME ["/app/data/raw"]

CMD ["python", "-u", "/app/src/main.py"]
