FROM python:3.11-slim

WORKDIR /app

# Build context is the repo root — copy only the dbt project files
COPY transformation/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY transformation/ .

# Runtime only — no pipeline dispatcher baked in.
# Pass the full command explicitly (absolute --profiles-dir — don't rely on
# WORKDIR staying /app if this Dockerfile changes later):
#   docker run dbt-pipeline:latest dbt build --profiles-dir /app/profiles
#   docker run dbt-pipeline:latest dbt build --select ecommerce --profiles-dir /app/profiles
#   docker run dbt-pipeline:latest dbt build --profiles-dir /app/profiles \
#     --vars '{"start_date": "2026-07-01", "end_date": "2026-07-20"}'
