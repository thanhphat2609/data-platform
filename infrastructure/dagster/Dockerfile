FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev ca-certificates curl && \
    # Docker CLI static binary — needed to run pipeline containers via subprocess
    curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-27.3.1.tgz \
    | tar xz --strip-components=1 -C /usr/local/bin docker/docker && \
    rm -rf /var/lib/apt/lists/*

COPY orchestration/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Orchestration code only — pipeline code lives in pipeline images, not here
COPY orchestration/ /opt/dagster/app/orchestration/

COPY infrastructure/dagster/dagster.yaml /opt/dagster/dagster_home/dagster.yaml
COPY infrastructure/dagster/workspace.yaml /opt/dagster/dagster_home/workspace.yaml

ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV PYTHONPATH=/opt/dagster/app/orchestration

WORKDIR /opt/dagster/app/orchestration
