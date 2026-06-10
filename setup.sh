#!/bin/bash

# Exit on error, undefined variables, and pipe failures
set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Function to check if command succeeded
check_success() {
    if [ $? -eq 0 ]; then
        print_status "$1 completed successfully"
    else
        print_error "$1 failed"
        exit 1
    fi
}

# Main script
print_status "Starting setup of real_time_platform..."

# Clone repository
if [ ! -d "real_time_platform" ]; then
    print_status "Cloning repository..."
    git clone https://github.com/SaeidZali/real_time_platform.git
    check_success "Git clone"
else
    print_warning "Directory real_time_platform already exists, skipping clone"
fi

# Enter directory
print_status "Entering real_time_platform directory..."
cd real_time_platform/
check_success "Changing directory"

# Create directories
print_status "Creating directories..."
mkdir -p connectors clickhouse/{data,logs}
mkdir -p ./data/kafka
mkdir -p oradata
mkdir -p miniodata
mkdir -p postgres_data
mkdir -p airflow/logs airflow/plugins
mkdir -p ./cloudbeaver_data
mkdir -p ./dremio/data ./dremio/logs
mkdir -p ./nessie-data
mkdir -p ./marquez/postgres
mkdir -p ./jars
check_success "Directory creation"

# Set permissions for data directory
print_status "Setting permissions for ./data..."
sudo chmod -R 755 ./data
sudo chmod -R 777 ./data
check_success "Data directory permissions"

# Set permissions for oradata
print_status "Setting permissions for oradata..."
sudo chown -R 54321:54321 oradata
chmod 775 oradata
check_success "Oradata permissions"

# Set permissions for miniodata
print_status "Setting permissions for miniodata..."
chmod 777 miniodata
check_success "Miniodata permissions"

# Set recursive permissions for superset
print_status "Setting recursive permissions for superset..."
sudo chmod 777 ./ -R
check_success "Superset permissions"

# Set permissions for dremio
print_status "Setting permissions for dremio..."
chmod 777 ./dremio/data ./dremio/logs
check_success "Dremio permissions"

# Set permissions for nessie-data
print_status "Setting permissions for nessie-data..."
chmod 777 ./nessie-data
check_success "Nessie-data permissions"

# Download OpenLineage JAR
print_status "Downloading OpenLineage Spark JAR..."
curl -L \
    --fail \
    --show-error \
    --progress-bar \
    https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.12/1.28.0/openlineage-spark_2.12-1.28.0.jar \
    -o jars/openlineage-spark.jar

if [ $? -eq 0 ]; then
    print_status "OpenLineage JAR downloaded successfully"
    ls -lh jars/openlineage-spark.jar
else
    print_error "Failed to download OpenLineage JAR"
    exit 1
fi

# Final summary
print_status "========================================="
print_status "Setup completed successfully!"
print_status "========================================="
print_status "Created directories:"
echo "  - connectors/"
echo "  - clickhouse/data/, clickhouse/logs/"
echo "  - data/kafka/"
echo "  - oradata/"
echo "  - miniodata/"
echo "  - postgres_data/"
echo "  - airflow/logs/, airflow/plugins/"
echo "  - cloudbeaver_data/"
echo "  - dremio/data/, dremio/logs/"
echo "  - nessie-data/"
echo "  - marquez/postgres/"
echo "  - jars/"
print_status "========================================="

# Optional: List directory contents
print_status "Current directory contents:"
ls -la

print_status "You are now in: $(pwd)"
print_status "To verify everything is set up correctly, run: ls -la"
