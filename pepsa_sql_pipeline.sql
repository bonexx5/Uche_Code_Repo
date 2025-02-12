# Here’s how you can combine the steps into a full SQL-based pipeline.

-- Step 1: Extract Data from MySQL
-- Run this in MySQL
SELECT shipment_id, customer_id, shipment_date, origin, destination, weight, status
FROM shipments
LIMIT 100000
INTO OUTFILE '/tmp/shipments_data.csv'
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n';

-- Step 2: Transform Data (Optional)
-- Use Python or another tool to transform the data as needed.

-- Step 3: Load Data into PostgreSQL
-- Run this in PostgreSQL
BEGIN;

-- Log the start of the migration
INSERT INTO data_migration_log (rows_migrated, status)
VALUES (0, 'STARTED');

-- Load the data
COPY shipments(shipment_id, customer_id, shipment_date, origin, destination, weight, status)
FROM '/path/to/transformed_shipments_data.csv'
WITH CSV HEADER;

-- Log the successful migration
INSERT INTO data_migration_log (rows_migrated, status)
VALUES (100000, 'SUCCESS');

COMMIT;

-- Error Handling
EXCEPTION WHEN OTHERS THEN
    -- Log the error
    INSERT INTO data_migration_log (rows_migrated, status, error_message)
    VALUES (0, 'FAILED', SQLERRM);
    ROLLBACK;

