#!/bin/bash
# Run this ONCE on the Master VM.
# Installs PostgreSQL, Redis, and Dagster webserver + daemon.
# Usage: bash setup.sh

set -e

PROJECT_DIR="/home/azureuser/dagster-demo"
VENV_DIR="$PROJECT_DIR/venv"
DB_NAME="dagster_db"
DB_USER="dagster"
DB_PASSWORD="changeme"       # ← change this

echo "========================================"
echo " Step 1: System packages"
echo "========================================"
sudo apt-get update -y
sudo apt-get install -y python3 python3-pip python3-venv git \
  postgresql postgresql-contrib \
  redis-server


echo "========================================"
echo " Step 2: Configure PostgreSQL"
echo "========================================"

# Enable and start the PostgreSQL service so it runs automatically on boot
sudo systemctl enable postgresql
sudo systemctl start postgresql

# Create a new database, user, and grant necessary privileges for Dagster
sudo -u postgres psql <<SQL
  CREATE USER $DB_USER WITH PASSWORD '$DB_PASSWORD';
  CREATE DATABASE $DB_NAME OWNER $DB_USER;
  GRANT ALL PRIVILEGES ON DATABASE $DB_NAME TO $DB_USER;
SQL

echo "==> Configuring PostgreSQL to accept external connections..."

# 1. Modify postgresql.conf to listen on all IP addresses instead of just localhost
# This uses regex to find and replace 'localhost' with '*' (handles both commented and uncommented lines)
sudo sed -i "s/^#listen_addresses = 'localhost'/listen_addresses = '*'/g" /etc/postgresql/*/main/postgresql.conf
sudo sed -i "s/^listen_addresses = 'localhost'/listen_addresses = '*'/g" /etc/postgresql/*/main/postgresql.conf

# 2. Append a rule to pg_hba.conf to allow password-based (md5) connections from any IP within the VNet (0.0.0.0/0)
echo "host    all             all             0.0.0.0/0               md5" | sudo tee -a /etc/postgresql/*/main/pg_hba.conf > /dev/null

# 3. Restart the PostgreSQL service to apply the new network configurations
sudo systemctl restart postgresql

echo "PostgreSQL ready: postgresql://$DB_USER:$DB_PASSWORD@localhost:5432/$DB_NAME"


echo "========================================"
echo " Step 3: Configure Redis"
echo "========================================"
# Bind to all interfaces so worker VMs can connect.
# Newer Redis configs use "bind 127.0.0.1 -::1" — replace the whole bind line.
sudo sed -i 's/^bind .*/bind 0.0.0.0/' /etc/redis/redis.conf

# Disable protected mode — it closes remote connections when no password is set.
# We rely on Azure NSG rules for network-level security instead.
sudo sed -i 's/^protected-mode yes/protected-mode no/' /etc/redis/redis.conf

sudo systemctl enable redis-server
sudo systemctl restart redis-server

sudo redis-cli CONFIG SET protected-mode no

echo "Redis ready: redis://localhost:6379/0"


echo "========================================"
echo " Step 4: Install Python dependencies"
echo "========================================"
python3 -m venv $VENV_DIR
$VENV_DIR/bin/pip install --upgrade pip
$VENV_DIR/bin/pip install -r $PROJECT_DIR/requirements.txt


echo "========================================"
echo " Done!"
echo "========================================"
echo ""
echo "Next steps:"
echo "  1. Update dagster.yaml with real credentials"
echo "  2. Run: bash control/start_control.sh"
echo "  OR register systemd services for auto-start"
