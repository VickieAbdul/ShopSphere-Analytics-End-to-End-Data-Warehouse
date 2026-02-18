--Create a new table in STAGING by selecting and transforming data from RAW
--Look at the date value and figure out which format it is, then convert it to a standard format
--TRY_TO_NUMBER and TRY_TO_DECIMAL replaced with ::INT and ::FLOAT This is because Snowflake already detected numbers during upload so we just need to confirm the format :: is Snowflake's shorthand for casting. quantity::INT means "treat quantity as an integer" ROUND(..., 2) ensures exactly 2 decimal places for prices
-------------------------------------------------------------------------------------------
USE DATABASE SHOPSPHERE_DB;
CREATE
OR REPLACE TABLE SCHEMA_STAGING.SALES_ORDERS AS
SELECT
    order_id,
    -- Standardize all date formats to YYYY-MM-DD
    CASE
        -- Already ISO format: YYYY-MM-DD
        WHEN order_date LIKE '____-__-__' THEN TRY_TO_DATE(order_date, 'YYYY-MM-DD') -- US format: MM/DD/YYYY (when day part > 12)
        WHEN order_date LIKE '__/__/____'
        AND SPLIT_PART(order_date, '/', 2)::INT > 12 THEN TRY_TO_DATE(order_date, 'MM/DD/YYYY') -- EU format: DD/MM/YYYY
        WHEN order_date LIKE '__/__/____' THEN TRY_TO_DATE(order_date, 'DD/MM/YYYY')
        ELSE NULL
    END AS order_date,
    customer_id,
    product_id,
    product_category,
    country,
    region,
    currency_code,
    sales_channel,
    payment_method,
    -- Snowflake already cast these to numbers during upload
    -- We just ensure correct decimal precision
    quantity::INT AS quantity,
    ROUND(unit_price_local::FLOAT, 2) AS unit_price_local,
    ROUND(cost_per_unit_local::FLOAT, 2) AS cost_per_unit_local,
    ROUND(discount_pct::FLOAT, 2) AS discount_pct,
    -- Metadata column
    CURRENT_TIMESTAMP AS staged_at
FROM
    SCHEMA_RAW.SALES_ORDERS;
------------------------------------------------------------------------------------------
    -- Preview the staging table
SELECT
    *
FROM
    SCHEMA_STAGING.SALES_ORDERS
LIMIT
    10;
-------------------------------------------------------------------------------------------
    -- Create Staging table for CUSTOMERS
    CREATE
    OR REPLACE TABLE SCHEMA_STAGING.CUSTOMERS AS
SELECT
    customer_id,
    customer_name,
    -- Lowercase email for consistency
    LOWER(email) AS email,
    gender,
    -- Standardize date formats
    CASE
        WHEN date_of_birth LIKE '____-__-__' THEN TRY_TO_DATE(date_of_birth, 'YYYY-MM-DD')
        WHEN date_of_birth LIKE '__/__/____'
        AND SPLIT_PART(date_of_birth, '/', 2)::INT > 12 THEN TRY_TO_DATE(date_of_birth, 'MM/DD/YYYY')
        WHEN date_of_birth LIKE '__/__/____' THEN TRY_TO_DATE(date_of_birth, 'DD/MM/YYYY')
        ELSE NULL
    END AS date_of_birth,
    CASE
        WHEN signup_date LIKE '____-__-__' THEN TRY_TO_DATE(signup_date, 'YYYY-MM-DD')
        WHEN signup_date LIKE '__/__/____'
        AND SPLIT_PART(signup_date, '/', 2)::INT > 12 THEN TRY_TO_DATE(signup_date, 'MM/DD/YYYY')
        WHEN signup_date LIKE '__/__/____' THEN TRY_TO_DATE(signup_date, 'DD/MM/YYYY')
        ELSE NULL
    END AS signup_date,
    country,
    city,
    -- Metadata
    CURRENT_TIMESTAMP AS staged_at
FROM
    SCHEMA_RAW.CUSTOMERS;
------------------------------------------------------------------------------------------
    -- Create Staging table for PRODUCTS
    CREATE
    OR REPLACE TABLE SCHEMA_STAGING.PRODUCTS AS
SELECT
    product_id,
    product_name,
    product_category,
    brand,
    supplier_name,
    -- Already a number, just round it
    ROUND(base_cost_usd, 2) AS base_cost_usd,
    -- Already a date, no conversion needed
    launch_date,
    -- Normalize is_active to boolean
    CASE
        WHEN UPPER(is_active) = 'TRUE' THEN TRUE
        WHEN UPPER(is_active) = 'FALSE' THEN FALSE
        ELSE NULL
    END AS is_active,
    -- Metadata
    CURRENT_TIMESTAMP AS staged_at
FROM
    SCHEMA_RAW.PRODUCTS;
-------------------------------------------------------------------------------------------
    -- Verify all staging tables
SELECT
    'SALES_ORDERS' AS table_name,
    COUNT(*) AS row_count
FROM
    SCHEMA_STAGING.SALES_ORDERS
UNION ALL
SELECT
    'CUSTOMERS',
    COUNT(*)
FROM
    SCHEMA_STAGING.CUSTOMERS
UNION ALL
SELECT
    'PRODUCTS',
    COUNT(*)
FROM
    SCHEMA_STAGING.PRODUCTS;