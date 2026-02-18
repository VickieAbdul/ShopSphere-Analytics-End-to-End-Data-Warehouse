--Step 1: Create the Core Fact Table (Sales with Dimensions Joined).This is the foundation. Joining everything together will matter in the calculated business metrics.
---------------------------------------------------------------------------------------------
USE DATABASE SHOPSPHERE_DB;
CREATE
OR REPLACE TABLE SCHEMA_ANALYTICS.FACT_SALES AS
SELECT
    -- Identifiers
    s.order_id,
    s.order_date,
    s.customer_id,
    s.product_id,
    -- Sales dimensions
    s.country,
    s.region,
    s.currency_code,
    s.sales_channel,
    s.payment_method,
    -- Product dimensions (joined from products table)
    p.product_name,
    p.product_category,
    p.brand,
    p.supplier_name,
    p.is_active,
    -- Customer dimensions (joined from customers table)
    c.customer_name,
    c.email,
    c.gender,
    c.age,
    c.days_as_customer,
    c.city AS customer_city,
    -- Transaction details
    s.quantity,
    s.unit_price_local,
    s.cost_per_unit_local,
    s.discount_pct,
    -- ── Calculated Business Metrics ────────────────────────────────
    -- Gross revenue (before discount)
    s.quantity * s.unit_price_local AS gross_revenue_local,
    -- Discount amount
    s.quantity * s.unit_price_local * s.discount_pct AS discount_amount_local,
    -- Net revenue (after discount)
    s.quantity * s.unit_price_local * (1 - s.discount_pct) AS net_revenue_local,
    -- Total cost
    s.quantity * s.cost_per_unit_local AS total_cost_local,
    -- Gross profit (net revenue - cost)
    (
        s.quantity * s.unit_price_local * (1 - s.discount_pct)
    ) - (s.quantity * s.cost_per_unit_local) AS gross_profit_local,
    -- Profit margin %
    CASE
        WHEN s.quantity * s.unit_price_local * (1 - s.discount_pct) > 0 THEN (
            (
                s.quantity * s.unit_price_local * (1 - s.discount_pct)
            ) - (s.quantity * s.cost_per_unit_local)
        ) / (
            s.quantity * s.unit_price_local * (1 - s.discount_pct)
        )
        ELSE 0
    END AS profit_margin_pct,
    -- Date dimensions for time-based analysis
    YEAR(s.order_date) AS order_year,
    MONTH(s.order_date) AS order_month,
    QUARTER(s.order_date) AS order_quarter,
    DAYOFWEEK(s.order_date) AS order_day_of_week,
    -- Metadata
    CURRENT_TIMESTAMP AS created_at
FROM
    SCHEMA_CLEAN.SALES_ORDERS s
    LEFT JOIN SCHEMA_CLEAN.PRODUCTS p ON s.product_id = p.product_id
    LEFT JOIN SCHEMA_CLEAN.CUSTOMERS c ON s.customer_id = c.customer_id;
    ---------------------------------------------------------------------------------------------
    -- Verify the fact table by summarizing major metrics
SELECT
    COUNT(*) AS total_transactions,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(DISTINCT product_id) AS unique_products,
    -- Check if joins worked (nulls mean join failed)
    SUM(
        CASE
            WHEN product_name IS NULL THEN 1
            ELSE 0
        END
    ) AS missing_product_info,
    SUM(
        CASE
            WHEN customer_name IS NULL THEN 1
            ELSE 0
        END
    ) AS missing_customer_info,
    -- Check calculated metrics look reasonable
    ROUND(SUM(net_revenue_local), 2) AS total_net_revenue,
    ROUND(SUM(gross_profit_local), 2) AS total_gross_profit,
    ROUND(AVG(profit_margin_pct) * 100, 2) AS avg_profit_margin_pct
FROM
    SCHEMA_ANALYTICS.FACT_SALES;
    --------------------------------------------------------------------------------------------
    -- Next will be to create a customer dimension table with:
    --1. RFM Segmentation (Recency, Frequency, Monetary)
    --2. Customer Lifetime Value (CLV)
    --3. Churn Risk Flags
    --4. Spending Behavior
    --This will be done by:
    --i. Segmenting customers into Premium, Regular, Budget
    --ii.Calculating Customer Lifetime Value (CLV)
    --iii.Flagging high-risk churn customers
    USE DATABASE SHOPSPHERE_DB;
CREATE
    OR REPLACE TABLE SCHEMA_ANALYTICS.DIM_CUSTOMER_SEGMENTS AS WITH customer_metrics AS (
        -- Calculate key metrics per customer (one row per customer)
        SELECT
            customer_id,
            MAX(customer_name) AS customer_name,
            MAX(email) AS email,
            MAX(gender) AS gender,
            MAX(age) AS age,
            MAX(country) AS primary_country,
            -- Country where they bought most
            MAX(customer_city) AS primary_city,
            MAX(days_as_customer) AS days_as_customer,
            -- RFM Metrics
            MAX(order_date) AS last_order_date,
            DATEDIFF('day', MAX(order_date), CURRENT_DATE) AS days_since_last_order,
            COUNT(DISTINCT order_id) AS total_orders,
            SUM(net_revenue_local) AS total_revenue,
            SUM(gross_profit_local) AS total_profit,
            AVG(net_revenue_local) AS avg_order_value,
            -- Spending patterns
            MIN(order_date) AS first_order_date,
            COUNT(DISTINCT product_category) AS categories_purchased,
            COUNT(DISTINCT country) AS countries_purchased_from,
            -- Channel preference
            MODE(sales_channel) AS preferred_channel,
            MODE(payment_method) AS preferred_payment
        FROM
            SCHEMA_ANALYTICS.FACT_SALES
        GROUP BY
            customer_id -- FIXED: Only group by customer_id
    ),
    -- Calculate percentiles for segmentation
    percentiles AS (
        SELECT
            PERCENTILE_CONT(0.33) WITHIN GROUP (
                ORDER BY
                    total_revenue
            ) AS revenue_p33,
            PERCENTILE_CONT(0.67) WITHIN GROUP (
                ORDER BY
                    total_revenue
            ) AS revenue_p67,
            PERCENTILE_CONT(0.33) WITHIN GROUP (
                ORDER BY
                    total_orders
            ) AS orders_p33,
            PERCENTILE_CONT(0.67) WITHIN GROUP (
                ORDER BY
                    total_orders
            ) AS orders_p67,
            PERCENTILE_CONT(0.50) WITHIN GROUP (
                ORDER BY
                    days_since_last_order
            ) AS recency_median
        FROM
            customer_metrics
    )
SELECT
    cm.*,
    -- Customer Lifetime Value (CLV) = total revenue they've generated
    cm.total_revenue AS customer_lifetime_value,
    -- Average profit per order
    cm.total_profit / NULLIF(cm.total_orders, 0) AS avg_profit_per_order,
    -- Customer tenure in months
    ROUND(cm.days_as_customer / 30.0, 1) AS tenure_months,
    -- ── SEGMENTATION ───────────────────────────────────────────
    -- Monetary Segment (based on total revenue)
    CASE
        WHEN cm.total_revenue >= p.revenue_p67 THEN 'High Spender'
        WHEN cm.total_revenue >= p.revenue_p33 THEN 'Medium Spender'
        ELSE 'Low Spender'
    END AS monetary_segment,
    -- Frequency Segment (based on order count)
    CASE
        WHEN cm.total_orders >= p.orders_p67 THEN 'Frequent Buyer'
        WHEN cm.total_orders >= p.orders_p33 THEN 'Regular Buyer'
        ELSE 'Occasional Buyer'
    END AS frequency_segment,
    -- Recency Segment (based on days since last order)
    CASE
        WHEN cm.days_since_last_order <= 30 THEN 'Active'
        WHEN cm.days_since_last_order <= 90 THEN 'Cooling'
        WHEN cm.days_since_last_order <= 180 THEN 'At Risk'
        ELSE 'Churned'
    END AS recency_segment,
    -- Combined RFM Segment (Premium, Regular, Budget)
    CASE
        WHEN cm.total_revenue >= p.revenue_p67
        AND cm.total_orders >= p.orders_p67
        AND cm.days_since_last_order <= 90 THEN 'Premium'
        WHEN cm.total_revenue >= p.revenue_p33
        AND cm.total_orders >= p.orders_p33
        AND cm.days_since_last_order <= 180 THEN 'Regular'
        ELSE 'Budget'
    END AS customer_segment,
    -- Churn Risk Flag
    CASE
        WHEN cm.days_since_last_order > 180 THEN 'High Risk'
        WHEN cm.days_since_last_order > 90 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END AS churn_risk,
    -- Metadata
    CURRENT_TIMESTAMP AS created_at
FROM
    customer_metrics cm
    CROSS JOIN percentiles p;
    -- Verify customer segments
SELECT
    COUNT(*) AS total_customers,
    -- Segment distribution
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
    -- Churn risk distribution
    COUNT(
        CASE
            WHEN churn_risk = 'High Risk' THEN 1
        END
    ) AS high_risk_churn,
    COUNT(
        CASE
            WHEN churn_risk = 'Medium Risk' THEN 1
        END
    ) AS medium_risk_churn,
    COUNT(
        CASE
            WHEN churn_risk = 'Low Risk' THEN 1
        END
    ) AS low_risk_churn,
    -- Financial metrics
    ROUND(SUM(customer_lifetime_value), 2) AS total_clv,
    ROUND(AVG(customer_lifetime_value), 2) AS avg_clv_per_customer,
    ROUND(AVG(total_orders), 1) AS avg_orders_per_customer
FROM
    SCHEMA_ANALYTICS.DIM_CUSTOMER_SEGMENTS;
    ----------------------------------------------------------------------------------------------
    -- Verify customer segments
SELECT
    COUNT(*) AS total_customers,
    -- Segment distribution
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
    -- Churn risk distribution
    COUNT(
        CASE
            WHEN churn_risk = 'High Risk' THEN 1
        END
    ) AS high_risk_churn,
    COUNT(
        CASE
            WHEN churn_risk = 'Medium Risk' THEN 1
        END
    ) AS medium_risk_churn,
    COUNT(
        CASE
            WHEN churn_risk = 'Low Risk' THEN 1
        END
    ) AS low_risk_churn,
    -- Financial metrics
    ROUND(SUM(customer_lifetime_value), 2) AS total_clv,
    ROUND(AVG(customer_lifetime_value), 2) AS avg_clv_per_customer,
    ROUND(AVG(total_orders), 1) AS avg_orders_per_customer
FROM
    SCHEMA_ANALYTICS.DIM_CUSTOMER_SEGMENTS;
    -----------------------------------------------------------------------------------------
    -- Check what's causing the duplication
SELECT
    customer_id,
    COUNT(*) AS times_appearing
FROM
    SCHEMA_ANALYTICS.DIM_CUSTOMER_SEGMENTS
GROUP BY
    customer_id
ORDER BY
    times_appearing DESC
LIMIT
    10;