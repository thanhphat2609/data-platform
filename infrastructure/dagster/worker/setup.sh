#!/bin/bash
# Run this ONCE on each worker VM to install all dependencies.
# Usage: bash setup.sh

set -e

PROJECT_DIR="/home/azureuser/dagster-demo"
VENV_DIR="$PROJECT_DIR/venv"

echo "==> Updating system packages..."
sudo apt-get update -y
sudo apt-get install -y python3 python3-pip python3-venv git

echo "==> Cloning project (skip if already cloned)..."
# git clone https://github.com/YOUR_ORG/dagster-demo.git $PROJECT_DIR
# cd $PROJECT_DIR

echo "==> Creating virtual environment..."
python3 -m venv $VENV_DIR

echo "==> Installing Python dependencies..."
$VENV_DIR/bin/pip install --upgrade pip
$VENV_DIR/bin/pip install -r $PROJECT_DIR/requirements.txt

echo "==> Done. Next: run start_worker1.sh or start_worker2.sh"
