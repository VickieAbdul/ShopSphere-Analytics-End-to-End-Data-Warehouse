--Before cleaning anything, let's profile the data first. This means understanding exactly how much bad data exists.
-----------------------------------------------------------------------------------------------
USE DATABASE SHOPSPHERE_DB;
-- Data Quality Audit on STAGING
SELECT
    -- Total records
    COUNT(*) AS total_records,
    -- Duplicate order_ids
    COUNT(*) - COUNT(DISTINCT order_id) AS duplicate_order_ids,
    -- NULL values per column
    SUM(
        CASE
            WHEN order_date IS NULL THEN 1
            ELSE 0
        END
    ) AS null_dates,
    SUM(
        CASE
            WHEN customer_id IS NULL THEN 1
            ELSE 0
        END
    ) AS null_customers,
    SUM(
        CASE
            WHEN product_id IS NULL THEN 1
            ELSE 0
        END
    ) AS null_products,
    SUM(
        CASE
            WHEN quantity IS NULL THEN 1
            ELSE 0
        END
    ) AS null_quantities,
    SUM(
        CASE
            WHEN discount_pct IS NULL THEN 1
            ELSE 0
        END
    ) AS null_discounts,
    SUM(
        CASE
            WHEN unit_price_local IS NULL THEN 1
            ELSE 0
        END
    ) AS null_prices,
    -- Negative quantities
    SUM(
        CASE
            WHEN quantity < 0 THEN 1
            ELSE 0
        END
    ) AS negative_quantities,
    -- Zero prices (suspicious)
    SUM(
        CASE
            WHEN unit_price_local = 0 THEN 1
            ELSE 0
        END
    ) AS zero_prices
FROM
    SCHEMA_STAGING.SALES_ORDERS;
-----------------------------------------------------------------------------------------------
    --Clean the sales_orders data
    USE DATABASE SHOPSPHERE_DB;
CREATE
    OR REPLACE TABLE SCHEMA_CLEAN.SALES_ORDERS AS -- Step 1: Remove duplicates by keeping the first occurrence
    -- ROW_NUMBER assigns 1 to the first time an order_id appears
    WITH deduplicated AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY
                    staged_at
            ) AS row_num
        FROM
            SCHEMA_STAGING.SALES_ORDERS
    ),
    -- Step 2: Keep only the first occurrence of each order_id
    first_occurrences AS (
        SELECT
            *
        FROM
            deduplicated
        WHERE
            row_num = 1
    ),
    -- Step 3: Apply all cleaning rules
    cleaned AS (
        SELECT
            order_id,
            order_date,
            customer_id,
            product_id,
            product_category,
            country,
            region,
            currency_code,
            sales_channel,
            payment_method,
            quantity,
            unit_price_local,
            cost_per_unit_local,
            -- Replace NULL discounts with 0
            COALESCE(discount_pct, 0) AS discount_pct,
            -- Metadata
            staged_at,
            CURRENT_TIMESTAMP AS cleaned_at
        FROM
            first_occurrences -- Step 4: Remove negative quantities
        WHERE
            quantity > 0
    )
SELECT
    *
FROM
    cleaned;
-------------------------------------------------------------------------------------------
    -- Verify CLEAN layer
SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT order_id) AS unique_order_ids,
    SUM(
        CASE
            WHEN quantity < 0 THEN 1
            ELSE 0
        END
    ) AS negative_quantities,
    SUM(
        CASE
            WHEN discount_pct IS NULL THEN 1
            ELSE 0
        END
    ) AS null_discounts,
    MIN(quantity) AS min_quantity,
    MAX(discount_pct) AS max_discount
FROM
    SCHEMA_CLEAN.SALES_ORDERS;
--------------------------------------------------------------------------------------------
    -- Clean Customers data
    CREATE
    OR REPLACE TABLE SCHEMA_CLEAN.CUSTOMERS AS
SELECT
    customer_id,
    customer_name,
    -- Trim any accidental spaces
    TRIM(email) AS email,
    gender,
    date_of_birth,
    signup_date,
    country,
    city,
    -- Calculate customer age (useful for analysis later)
    DATEDIFF('year', date_of_birth, CURRENT_DATE) AS age,
    -- How long have they been a customer (in days)
    DATEDIFF('day', signup_date, CURRENT_DATE) AS days_as_customer,
    -- Metadata
    staged_at,
    CURRENT_TIMESTAMP AS cleaned_at
FROM
    SCHEMA_STAGING.CUSTOMERS -- Remove any records with missing critical fields
WHERE
    customer_id IS NOT NULL
    AND customer_name IS NOT NULL
    AND email IS NOT NULL;
-- Clean Products Table
    CREATE
    OR REPLACE TABLE SCHEMA_CLEAN.PRODUCTS AS
SELECT
    product_id,
    -- Trim any accidental spaces in names
    TRIM(product_name) AS product_name,
    product_category,
    brand,
    supplier_name,
    base_cost_usd,
    launch_date,
    is_active,
    -- How many days since product launched
    DATEDIFF('day', launch_date, CURRENT_DATE) AS days_since_launch,
    -- Metadata
    staged_at,
    CURRENT_TIMESTAMP AS cleaned_at
FROM
    SCHEMA_STAGING.PRODUCTS -- Only keep valid products
WHERE
    product_id IS NOT NULL
    AND product_name IS NOT NULL;
---------------------------------------------------------------------------------------------
    -- Verify all CLEAN tables
SELECT
    'SALES_ORDERS' AS table_name,
    COUNT(*) AS row_count
FROM
    SCHEMA_CLEAN.SALES_ORDERS
UNION ALL
SELECT
    'CUSTOMERS',
    COUNT(*)
FROM
    SCHEMA_CLEAN.CUSTOMERS
UNION ALL
SELECT
    'PRODUCTS',
    COUNT(*)
FROM
    SCHEMA_CLEAN.PRODUCTS;