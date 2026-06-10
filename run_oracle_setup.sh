#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Check if container is running
print_status "Checking if dbz_oracle19 container is running..."
if ! docker ps | grep -q dbz_oracle19; then
    print_error "Container dbz_oracle19 is not running!"
    print_status "Please start the container first with: docker start dbz_oracle19"
    exit 1
fi

# Wait for Oracle to be ready
print_status "Waiting for Oracle to be ready..."
sleep 5

# Script 1: Create user and grant privileges
print_status "Running script 1: Creating c##dbzuser user and granting privileges..."
print_status "Executing: docker exec -i -e ORACLE_SID=ORCLCDB dbz_oracle19 sqlplus sys/oraclepw@ORCLCDB AS SYSDBA < create_oracle_user.sql"

docker exec -i -e ORACLE_SID=ORCLCDB dbz_oracle19 sqlplus sys/oraclepw@ORCLCDB AS SYSDBA < create_oracle_user.sql

if [ $? -eq 0 ]; then
    print_status "User creation and privilege grant completed successfully!"
else
    print_error "Failed to create user or grant privileges"
    exit 1
fi

echo ""
print_status "Waiting 3 seconds before next script..."
sleep 3
echo ""

# Script 2: Create table and insert data
print_status "Running script 2: Creating customers table and inserting data..."
print_status "Executing: docker exec -i -e ORACLE_SID=ORCLPDB1 dbz_oracle19 sqlplus c##dbzuser/dbz@ORCLPDB1 < create_customers_table.sql"

docker exec -i -e ORACLE_SID=ORCLPDB1 dbz_oracle19 sqlplus c##dbzuser/dbz@ORCLPDB1 < create_customers_table.sql

if [ $? -eq 0 ]; then
    print_status "Table creation and data insertion completed successfully!"
else
    print_error "Failed to create table or insert data"
    exit 1
fi

print_status "========================================="
print_status "Oracle setup completed successfully!"
print_status "========================================="
print_status "Summary:"
echo "  ✓ User c##dbzuser created"
echo "  ✓ All privileges granted"
echo "  ✓ Customers table created in ORCLPDB1"
echo "  ✓ 4 sample records inserted"
echo "  ✓ Supplemental logging enabled"
print_status "========================================="
