-- create_customers_table.sql
-- Run this script as c##dbzuser in the PDB container
-- Usage: docker exec -i -e ORACLE_SID=ORCLPDB1 dbz_oracle19 sqlplus c##dbzuser/dbz@ORCLPDB1 < create_customers_table.sql

WHENEVER SQLERROR EXIT SQL.SQLCODE;

PROMPT Connecting to ORCLPDB1...
PROMPT Creating customers table...

-- Create the customers table
CREATE TABLE customers (
    id NUMBER(9,0) PRIMARY KEY, 
    name VARCHAR2(50)
);

PROMPT Inserting sample data...

-- Insert sample records
INSERT INTO customers VALUES (1001, 'Salles Thomas');
INSERT INTO customers VALUES (1002, 'George Bailey');
INSERT INTO customers VALUES (1003, 'Edward Walker');
INSERT INTO customers VALUES (1004, 'Anne Kretchmar');

-- Commit the transaction
COMMIT;

PROMPT Enabling supplemental logging...

-- Enable supplemental logging for all columns
ALTER TABLE customers ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

PROMPT Verifying the data...
SELECT COUNT(*) AS total_records FROM customers;
SELECT * FROM customers ORDER BY id;

PROMPT Table customers created successfully with 4 records!
EXIT;
