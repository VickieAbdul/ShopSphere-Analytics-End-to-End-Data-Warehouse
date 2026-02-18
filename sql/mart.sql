--Create Mart Table 1 (Regional Performance)
USE DATABASE SHOPSPHERE_DB;
CREATE
OR REPLACE TABLE SCHEMA_MART.MART_REGIONAL_PERFORMANCE AS
SELECT
    region,
    -- Transaction metrics
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(DISTINCT product_id) AS unique_products_sold,
    -- Revenue metrics
    ROUND(SUM(gross_revenue_local), 2) AS gross_revenue,
    ROUND(SUM(discount_amount_local), 2) AS total_discounts_given,
    ROUND(SUM(net_revenue_local), 2) AS net_revenue,
    -- Cost and profit
    ROUND(SUM(total_cost_local), 2) AS total_cost,
    ROUND(SUM(gross_profit_local), 2) AS gross_profit,
    -- Profitability metrics
    ROUND(AVG(profit_margin_pct) * 100, 2) AS avg_profit_margin_pct,
    ROUND(
        SUM(gross_profit_local) / NULLIF(SUM(net_revenue_local), 0) * 100,
        2
    ) AS overall_profit_margin_pct,
    -- Average order value
    ROUND(AVG(net_revenue_local), 2) AS avg_order_value,
    -- Discount rate
    ROUND(AVG(discount_pct) * 100, 2) AS avg_discount_rate_pct,
    -- Performance ranking
    RANK() OVER (
        ORDER BY
            SUM(gross_profit_local) DESC
    ) AS profit_rank,
    -- Metadata
    CURRENT_TIMESTAMP AS created_at
FROM
    SCHEMA_ANALYTICS.FACT_SALES
GROUP BY
    region
ORDER BY
    gross_profit DESC;
-----------------------------------------------------------------------------------
    -- Create Mart Table 2 (Customer Segments Summary)
    USE DATABASE SHOPSPHERE_DB;
CREATE
    OR REPLACE TABLE SCHEMA_MART.MART_CUSTOMER_SEGMENTS AS
SELECT
    customer_segment,
    -- Customer counts
    COUNT(DISTINCT customer_id) AS total_customers,
    -- Customer characteristics
    ROUND(AVG(age), 1) AS avg_age,
    ROUND(AVG(tenure_months), 1) AS avg_tenure_months,
    -- Purchase behavior
    ROUND(AVG(total_orders), 1) AS avg_orders_per_customer,
    ROUND(AVG(categories_purchased), 1) AS avg_categories_purchased,
    -- Financial metrics
    ROUND(SUM(customer_lifetime_value), 2) AS total_clv,
    ROUND(AVG(customer_lifetime_value), 2) AS avg_clv_per_customer,
    ROUND(AVG(avg_order_value), 2) AS avg_order_value,
    ROUND(SUM(total_profit), 2) AS total_profit,
    ROUND(AVG(avg_profit_per_order), 2) AS avg_profit_per_order,
    -- Recency
    ROUND(AVG(days_since_last_order), 1) AS avg_days_since_last_order,
    -- Contribution
    ROUND(
        SUM(customer_lifetime_value) / SUM(SUM(customer_lifetime_value)) OVER () * 100,
        2
    ) AS clv_contribution_pct,
    -- Channel preference
    MODE(preferred_channel) AS most_common_channel,
    MODE(preferred_payment) AS most_common_payment,
    -- Metadata
    CURRENT_TIMESTAMP AS created_at
FROM
    SCHEMA_ANALYTICS.DIM_CUSTOMER_SEGMENTS
GROUP BY
    customer_segment
ORDER BY
    total_clv DESC;
-----------------------------------------------------------------------------
    --Create Mart Table 3 (Discount Impact)
    USE DATABASE SHOPSPHERE_DB;
CREATE
    OR REPLACE TABLE SCHEMA_MART.MART_DISCOUNT_IMPACT AS
SELECT
    -- Discount buckets
    CASE
        WHEN discount_pct = 0 THEN 'No Discount'
        WHEN discount_pct <= 0.10 THEN '1-10% Discount'
        WHEN discount_pct <= 0.20 THEN '11-20% Discount'
        WHEN discount_pct <= 0.30 THEN '21-30% Discount'
        ELSE '30%+ Discount'
    END AS discount_bucket,
    -- Transaction counts
    COUNT(DISTINCT order_id) AS total_orders,
    -- Revenue impact
    ROUND(SUM(gross_revenue_local), 2) AS gross_revenue,
    ROUND(SUM(discount_amount_local), 2) AS total_discount_amount,
    ROUND(SUM(net_revenue_local), 2) AS net_revenue,
    -- Profitability impact
    ROUND(SUM(gross_profit_local), 2) AS gross_profit,
    ROUND(AVG(profit_margin_pct) * 100, 2) AS avg_profit_margin_pct,
    -- Average metrics
    ROUND(AVG(discount_pct) * 100, 2) AS avg_discount_pct,
    ROUND(AVG(net_revenue_local), 2) AS avg_order_value,
    -- Volume
    SUM(quantity) AS total_units_sold,
    -- Revenue per order
    ROUND(
        SUM(net_revenue_local) / NULLIF(COUNT(DISTINCT order_id), 0),
        2
    ) AS revenue_per_order,
    -- Metadata
    CURRENT_TIMESTAMP AS created_at
FROM
    SCHEMA_ANALYTICS.FACT_SALES
GROUP BY
    discount_bucket
ORDER BY
    CASE
        discount_bucket
        WHEN 'No Discount' THEN 1
        WHEN '1-10% Discount' THEN 2
        WHEN '11-20% Discount' THEN 3
        WHEN '21-30% Discount' THEN 4
        ELSE 5
    END;
----------------------------------------------------------------------------------
    --Create Mart Table 4 (Product Performance Table)
    USE DATABASE SHOPSPHERE_DB;
CREATE
    OR REPLACE TABLE SCHEMA_MART.MART_PRODUCT_PERFORMANCE AS
SELECT
    product_category,
    -- Volume metrics
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(quantity) AS total_units_sold,
    COUNT(DISTINCT customer_id) AS unique_customers,
    -- Revenue metrics
    ROUND(SUM(net_revenue_local), 2) AS net_revenue,
    ROUND(SUM(gross_profit_local), 2) AS gross_profit,
    -- Per-unit metrics
    ROUND(AVG(unit_price_local), 2) AS avg_unit_price,
    ROUND(AVG(cost_per_unit_local), 2) AS avg_unit_cost,
    -- Profitability
    ROUND(AVG(profit_margin_pct) * 100, 2) AS avg_profit_margin_pct,
    -- Revenue contribution
    ROUND(
        SUM(net_revenue_local) / SUM(SUM(net_revenue_local)) OVER () * 100,
        2
    ) AS revenue_contribution_pct,
    -- Volume contribution
    ROUND(
        SUM(quantity) / SUM(SUM(quantity)) OVER () * 100,
        2
    ) AS volume_contribution_pct,
    -- Revenue per unit sold
    ROUND(
        SUM(net_revenue_local) / NULLIF(SUM(quantity), 0),
        2
    ) AS revenue_per_unit,
    -- Performance ranking
    RANK() OVER (
        ORDER BY
            SUM(net_revenue_local) DESC
    ) AS revenue_rank,
    RANK() OVER (
        ORDER BY
            SUM(quantity) DESC
    ) AS volume_rank,
    -- Metadata
    CURRENT_TIMESTAMP AS created_at
FROM
    SCHEMA_ANALYTICS.FACT_SALES
GROUP BY
    product_category
ORDER BY
    net_revenue DESC;
------------------------------------------------------------------------------------------
    --MART Table 5: Retention & Churn Metrics.
    --This table will answer the CEO's Question: "How can we reduce churn?"
    USE DATABASE SHOPSPHERE_DB;
CREATE
    OR REPLACE TABLE SCHEMA_MART.MART_RETENTION_METRICS AS
SELECT
    churn_risk,
    -- Customer counts
    COUNT(DISTINCT customer_id) AS total_customers,
    -- Percentage of customer base
    ROUND(
        COUNT(DISTINCT customer_id) / SUM(COUNT(DISTINCT customer_id)) OVER () * 100,
        2
    ) AS pct_of_customer_base,
    -- Segment distribution within churn risk
    COUNT(
        CASE
            WHEN customer_segment = 'Premium' THEN 1
        END
    ) AS premium_customers,
    COUNT(
        CASE
            WHEN customer_segment = 'Regular' THEN 1
        END
    ) AS regular_customers,
    COUNT(
        CASE
            WHEN customer_segment = 'Budget' THEN 1
        END
    ) AS budget_customers,
    -- Financial risk
    ROUND(SUM(customer_lifetime_value), 2) AS total_clv_at_risk,
    ROUND(AVG(customer_lifetime_value), 2) AS avg_clv,
    -- Recency metrics
    ROUND(AVG(days_since_last_order), 1) AS avg_days_since_last_order,
    MIN(days_since_last_order) AS min_days_since_last_order,
    MAX(days_since_last_order) AS max_days_since_last_order,
    -- Purchase behavior
    ROUND(AVG(total_orders), 1) AS avg_lifetime_orders,
    ROUND(AVG(avg_order_value), 2) AS avg_order_value,
    -- Metadata
    CURRENT_TIMESTAMP AS created_at
FROM
    SCHEMA_ANALYTICS.DIM_CUSTOMER_SEGMENTS
GROUP BY
    churn_risk
ORDER BY
    CASE
        churn_risk
        WHEN 'High Risk' THEN 1
        WHEN 'Medium Risk' THEN 2
        ELSE 3
    END;
---------------------------------------------------------------------------------
    -- Verify all MART tables exist and have data
SELECT
    'MART_REGIONAL_PERFORMANCE' AS table_name,
    COUNT(*) AS row_count
FROM
    SCHEMA_MART.MART_REGIONAL_PERFORMANCE
UNION ALL
SELECT
    'MART_PRODUCT_PERFORMANCE',
    COUNT(*)
FROM
    SCHEMA_MART.MART_PRODUCT_PERFORMANCE
UNION ALL
SELECT
    'MART_DISCOUNT_IMPACT',
    COUNT(*)
FROM
    SCHEMA_MART.MART_DISCOUNT_IMPACT
UNION ALL
SELECT
    'MART_CUSTOMER_SEGMENTS',
    COUNT(*)
FROM
    SCHEMA_MART.MART_CUSTOMER_SEGMENTS
UNION ALL
SELECT
    'MART_RETENTION_METRICS',
    COUNT(*)
FROM
    SCHEMA_MART.MART_RETENTION_METRICS;