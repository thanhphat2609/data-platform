FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev && rm -rf /var/lib/apt/lists/*

# Install Python deps from orchestration layer
# (includes dlt, Docker SDK, dagster, azure-storage-blob, pandas, etc.)
COPY orchestration/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Install dlt pipeline-specific deps (psycopg2 for postgres_pipeline)
COPY ingestion/dlt/requirements.txt /tmp/dlt_requirements.txt
RUN pip install --no-cache-dir -r /tmp/dlt_requirements.txt

# Copy orchestration code
COPY orchestration/ /opt/dagster/app/orchestration/

# Copy dlt ingestion code — imported by dlt Dagster assets at runtime
COPY ingestion/dlt/ /opt/dagster/app/ingestion/dlt/

# Copy Dagster instance config and workspace into DAGSTER_HOME
COPY infrastructure/dagster/dagster.yaml /opt/dagster/dagster_home/dagster.yaml
COPY infrastructure/dagster/workspace.yaml /opt/dagster/dagster_home/workspace.yaml

ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV PYTHONPATH=/opt/dagster/app/orchestration:/opt/dagster/app/ingestion/dlt

WORKDIR /opt/dagster/app/orchestration
