FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev && rm -rf /var/lib/apt/lists/*

# Install Python deps from orchestration layer
COPY orchestration/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy orchestration code
COPY orchestration/ /opt/dagster/app/orchestration/

# Copy Dagster instance config and workspace into DAGSTER_HOME
COPY infrastructure/dagster/dagster.yaml /opt/dagster/dagster_home/dagster.yaml
COPY infrastructure/dagster/workspace.yaml /opt/dagster/dagster_home/workspace.yaml

ENV DAGSTER_HOME=/opt/dagster/dagster_home
# Makes `from assets import ...` etc. work for Celery workers
ENV PYTHONPATH=/opt/dagster/app/orchestration

WORKDIR /opt/dagster/app/orchestration
