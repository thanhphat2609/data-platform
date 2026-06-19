#!/bin/bash
# Start Dagster webserver and daemon on the Master VM.
# Run in two separate terminals, or use systemd services instead.

export PROJECT_DIR="/home/azureuser/dagster-demo"
export VENV_DIR="$PROJECT_DIR/venv"

cd $PROJECT_DIR

$VENV_DIR/bin/dagster dev -h 0.0.0.0 -p 3000
echo "Starting Dagster Webserver on port 3000..."
echo "Starting Dagster Daemon..."