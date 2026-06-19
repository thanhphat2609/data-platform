#!/bin/bash
# Start Celery worker-1 — handles sap_pipeline (queue: worker-1)
# Run this on Worker VM-1

export PROJECT_DIR="/home/azureuser/dagster-demo"
export VENV_DIR="$PROJECT_DIR/venv"
export DAGSTER_HOME="$PROJECT_DIR/dagster_home"

# Set this to your Master VM's private IP
export MASTER_VM_IP="<Master_IP_VM>"

# dagster-celery does NOT read the broker URL from the -y YAML file.
# Celery reads it from this env var at worker startup.
export CELERY_BROKER_URL="redis://${MASTER_VM_IP}:6379/0"
export CELERY_RESULT_BACKEND="redis://${MASTER_VM_IP}:6379/0"

cd $PROJECT_DIR

echo "==> Starting Dagster Celery Worker-1 (queue: worker-1, broker: $CELERY_BROKER_URL)..."
$VENV_DIR/bin/dagster-celery worker start -y $PROJECT_DIR/worker/celery_worker.yaml -A dagster_celery.app -q worker-1
