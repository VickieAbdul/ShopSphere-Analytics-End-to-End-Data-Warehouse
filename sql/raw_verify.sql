-- Verify all tables loaded correctly
USE DATABASE SHOPSPHERE_DB;
-- Check row counts
SELECT
    'SALES_ORDERS' AS table_name,
    COUNT(*) AS row_count
FROM
    SCHEMA_RAW.SALES_ORDERS
UNION ALL
SELECT
    'CUSTOMERS',
    COUNT(*)
FROM
    SCHEMA_RAW.CUSTOMERS
UNION ALL
SELECT
    'PRODUCTS',
    COUNT(*)
FROM
    SCHEMA_RAW.PRODUCTS;
-----------------------------------------------------------------------------------
    --CUSTOMERS shows 501 instead of 500.This means one extra row snuck in, most likely the header row was loaded as a record. Let's verify this.
    -- Check if header row was loaded as data
SELECT
    *
FROM
    SCHEMA_RAW.CUSTOMERS
LIMIT
    5;
--Header was loaded as first row data, so we can delete table and reload.
    --This time we create the column names forst via SQL before uploading csv
    CREATE
    OR REPLACE TABLE SCHEMA_RAW.CUSTOMERS (
        customer_id VARCHAR(50),
        customer_name VARCHAR(100),
        email VARCHAR(100),
        gender VARCHAR(10),
        date_of_birth VARCHAR(50),
        signup_date VARCHAR(50),
        country VARCHAR(50),
        city VARCHAR(50)
    );
--------------------------------------------------------------------------------
    -- Check again if header row was loaded as data
SELECT
    *
FROM
    SCHEMA_RAW.CUSTOMERS;