FROM python:3.11-slim

WORKDIR /app

# Build context is the repo root — copy only dlt source files
COPY ingestion/dlt/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingestion/dlt/.dlt/config.toml .dlt/config.toml
COPY ingestion/dlt/pipelines ./pipelines
