# ShopSphere Analytics: End-to-End Data Warehouse for E-Commerce Intelligence

## Project Overview

This is a production-grade analytics engineering project demonstrating enterprise data warehouse design for a fictional global e-commerce company (ShopSphere). This project showcases the complete data lifecycle from raw ingestion to executive-ready KPI dashboards, following industry best practices for data modeling, quality assurance, and business intelligence.

**Key Achievement:** Built a 5-layer data architecture processing 20,000+ transactions across 8 countries, answering critical business questions around profitability, product performance, discount strategy, customer segmentation, and churn risk.

---

## Business Problem

**Company:** ShopSphere - Global e-commerce company selling Electronics, Fashion, Home, Beauty, and Sports products across North America, Europe, Asia-Pacific, Latin America, and Middle East.

**Challenge:** The company recently migrated to Snowflake but lacked a structured analytics layer. The CEO needed answers to 5 critical questions:

1. **Are we actually profitable per region?**
2. **Which product categories drive revenue vs just volume?**
3. **Is discounting hurting margins?**
4. **Which customer segments are most valuable?**
5. **How can we reduce churn?**

**Solution:** Designed and implemented a complete analytics engineering pipeline with data quality checks, dimensional modeling, and pre-aggregated KPI tables optimized for BI dashboard consumption.

---

## Architecture

### 5-Layer Data Warehouse Design

```
┌─────────────────────────────────────────────────────────────┐
│                     RAW LAYER                                │
│  • Untouched source data (20K orders, 500 customers, 200    │
│    products)                                                 │
│  • All columns VARCHAR for maximum flexibility              │
│  • Audit trail and source of truth                          │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│                  STAGING LAYER                               │
│  • Standardized data types and formats                       │
│  • Date normalization (MM/DD/YYYY, DD/MM/YYYY → YYYY-MM-DD) │
│  • Email lowercasing, boolean conversion                     │
│  • No quality filtering (structure only)                     │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│                   CLEAN LAYER                                │
│  • Data quality enforcement                                  │
│  • Removed 386 duplicate order_ids                           │
│  • Removed 197 negative quantities (invalid transactions)    │
│  • Replaced 584 NULL discounts with 0                        │
│  • Result: 19,422 clean transactions                         │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│                ANALYTICS LAYER                               │
│  • FACT_SALES: Joined dimensions with business calculations  │
│    - Revenue, profit, margins calculated per transaction     │
│    - Date dimensions for time-series analysis                │
│  • DIM_CUSTOMER_SEGMENTS: RFM segmentation                   │
│    - Customer Lifetime Value (CLV)                           │
│    - Churn risk classification                               │
│    - Premium/Regular/Budget segments                         │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│                   MART LAYER                                 │
│  • Pre-aggregated executive KPI tables                       │
│  • 5 tables answering CEO's 5 questions                      │
│  • Optimized for BI dashboard consumption                    │
│  • Low latency, small row counts                             │
└─────────────────────────────────────────────────────────────┘
```

---

## Key Business Insights

### Overall Performance
- **Total Revenue:** $55.26M across 19,422 transactions
- **Gross Profit:** $18.74M
- **Average Profit Margin:** 39.43% (excellent for e-commerce)
- **Customer Base:** 500 active customers
- **Average Customer Lifetime Value:** $110,523
- **Repeat Purchase Rate:** 38.8 orders per customer

### Regional Profitability
<img width="1014" height="391" alt="image" src="https://github.com/user-attachments/assets/65c2c203-8765-4d58-9d84-2bbb6d65bcd0" />

- All 5 regions are profitable
- Profit margins range from 35-42%
- North America leads in total revenue
- Middle East has highest profit margins

### Product Performance
<img width="595" height="425" alt="image" src="https://github.com/user-attachments/assets/90efc680-13a9-44e0-9acf-509c71779374" />

- **Electronics:** Drives 35% of revenue (highest margin category)
- **Fashion:** Drives 25% of revenue (highest volume)
- **Revenue vs Volume Analysis:** Electronics brings less volume but more profit

### Discount Impact
<img width="731" height="426" alt="image" src="https://github.com/user-attachments/assets/af5a4fb3-c4ca-4ba4-a212-ba13c65b51ff" />

- **Finding:** Discounting does NOT significantly hurt margins
- 21-30% discounts still maintain 35%+ profit margins
- No discount orders have 41% margin vs 35% for discounted
- **Recommendation:** Current discount strategy is sustainable

### Customer Segmentation
<img width="850" height="428" alt="image" src="https://github.com/user-attachments/assets/0971995c-db6f-4b86-b28d-e9dc5216708f" />

- **Premium Customers:** 22% of base (110 customers) generate 45% of CLV
- **Regular Customers:** 33% of base (165 customers) generate 35% of CLV
- **Budget Customers:** 45% of base (225 customers) generate 20% of CLV
- **Pareto Principle Validated:** Top 22% drive nearly half the revenue

### Churn Risk
<img width="608" height="428" alt="image" src="https://github.com/user-attachments/assets/c56809eb-ca83-4608-aaa1-f2023b4ac3d4" />

- **Excellent News:** 98.8% of customers are low-risk (active)
- Only 6 customers (1.2%) at medium risk
- 0 customers at high risk
- **Recommendation:** Minimal churn prevention needed, focus on retention of Premium segment

---

## Technical Implementation

### Technologies Used
- **Database:** Snowflake Cloud Data Warehouse
- **Language:** SQL (Snowflake SQL dialect)
- **Visualization:** Python (matplotlib, plotly, pandas)
- **Data Volume:** 20,000+ transactions, 500 customers, 200 products
- **Geographic Coverage:** 8 countries across 5 regions

---

## Repository Structure

```
ShopSphere-Analytics/
│
├── README.md                          # This file
│
├── data/                              # Sample datasets
│   ├── sales_orders_usa.csv
│   ├── sales_orders_uk.csv
│   ├── sales_orders_germany.csv
│   ├── sales_orders_canada.csv
│   ├── sales_orders_australia.csv
│   ├── sales_orders_uae.csv
│   ├── sales_orders_singapore.csv
│   ├── sales_orders_brazil.csv
│   ├── customers.csv
│   └── products.csv
│
├── sql/                               # SQL scripts by layer
│   ├── 01_raw_layer/
│   │   ├── create_tables.sql
│   │   └── load_data.sql
│   ├── 02_staging_layer/
│   │   └── create_staging_tables.sql
│   ├── 03_clean_layer/
│   │   ├── data_quality_audit.sql
│   │   └── create_clean_tables.sql
│   ├── 04_analytics_layer/
│   │   ├── fact_sales.sql
│   │   └── dim_customer_segments.sql
│   └── 05_mart_layer/
│       ├── mart_regional_performance.sql
│       ├── mart_product_performance.sql
│       ├── mart_discount_impact.sql
│       ├── mart_customer_segments.sql
│       └── mart_retention_metrics.sql
│
├── analysis/                          # Business analysis queries
│   ├── ceo_question_1_regional_profitability.sql
│   ├── ceo_question_2_product_performance.sql
│   ├── ceo_question_3_discount_impact.sql
│   ├── ceo_question_4_customer_segments.sql
│   └── ceo_question_5_churn_analysis.sql
│
├── visualizations/                    # Generated charts
│   ├── generate_visualizations.py
│   └── charts/
│       ├── regional_performance.png
│       ├── product_performance.png
│       ├── discount_impact.png
│       ├── customer_segments.png
│       └── churn_risk.png
│
└── docs/
    ├── architecture_decisions.md
    ├── data_quality_report.md
    └── business_insights.md
```

---

## How to Replicate This Project

### Prerequisites
- Snowflake account (free trial available)
- Basic SQL knowledge
- Python 3.8+ (for visualizations, optional)

### Step-by-Step Setup

#### 1. Set Up Snowflake Database
```sql
-- Create database and schemas
CREATE DATABASE SHOPSPHERE_DB;

CREATE SCHEMA SHOPSPHERE_DB.SCHEMA_RAW;
CREATE SCHEMA SHOPSPHERE_DB.SCHEMA_STAGING;
CREATE SCHEMA SHOPSPHERE_DB.SCHEMA_CLEAN;
CREATE SCHEMA SHOPSPHERE_DB.SCHEMA_ANALYTICS;
CREATE SCHEMA SHOPSPHERE_DB.SCHEMA_MART;
```

#### 2. Load Raw Data
- Download CSV files from `/data/` folder
- Upload to Snowflake via Snowsight UI or SnowSQL
- Run scripts in `/sql/01_raw_layer/`

#### 3. Execute Transformations
Run SQL scripts in order:
```bash
sql/02_staging_layer/create_staging_tables.sql
sql/03_clean_layer/create_clean_tables.sql
sql/04_analytics_layer/fact_sales.sql
sql/04_analytics_layer/dim_customer_segments.sql
sql/05_mart_layer/*.sql
```

#### 4. Generate Visualizations (Optional)
```bash
cd visualizations
pip install -r requirements.txt
python generate_visualizations.py
```

#### 5. Run Analysis Queries
Execute queries in `/analysis/` folder to answer business questions

---

## Sample Queries

### Query 1: Regional Profitability Analysis
```sql
SELECT 
    region,
    net_revenue,
    gross_profit,
    overall_profit_margin_pct,
    profit_rank
FROM SCHEMA_MART.MART_REGIONAL_PERFORMANCE
ORDER BY gross_profit DESC;
```

**Business Answer:** "All regions are profitable. North America leads with $X in profit, followed by Europe. Middle East has the highest margin at 42%."

### Query 2: Product Revenue vs Volume
```sql
SELECT 
    product_category,
    revenue_contribution_pct,
    volume_contribution_pct,
    revenue_rank,
    volume_rank
FROM SCHEMA_MART.MART_PRODUCT_PERFORMANCE
ORDER BY net_revenue DESC;
```

**Business Answer:** "Electronics drives 35% of revenue but only 25% of volume - it's our most profitable category per unit."

### Query 3: Discount Impact on Margins
```sql
SELECT 
    discount_bucket,
    total_orders,
    avg_profit_margin_pct,
    revenue_per_order
FROM SCHEMA_MART.MART_DISCOUNT_IMPACT
ORDER BY discount_bucket;
```

**Business Answer:** "Discounting is not hurting margins significantly. Even 21-30% discounts maintain 35%+ profit margins. Strategy is sustainable."

### Query 4: Customer Segment Value
```sql
SELECT 
    customer_segment,
    total_customers,
    avg_clv_per_customer,
    clv_contribution_pct,
    avg_orders_per_customer
FROM SCHEMA_MART.MART_CUSTOMER_SEGMENTS
ORDER BY total_clv DESC;
```

**Business Answer:** "Premium customers (22% of base) generate 45% of total CLV with $XXX average value. Focus retention efforts here."

### Query 5: Churn Risk Assessment
```sql
SELECT 
    churn_risk,
    total_customers,
    pct_of_customer_base,
    total_clv_at_risk,
    premium_customers
FROM SCHEMA_MART.MART_RETENTION_METRICS
ORDER BY 
    CASE churn_risk
        WHEN 'High Risk' THEN 1
        WHEN 'Medium Risk' THEN 2
        ELSE 3
    END;
```

**Business Answer:** "Excellent retention: 98.8% of customers are active (low risk). Only 6 customers need re-engagement campaigns."

---

## Future Enhancements

Potential extensions to demonstrate additional skills:

- [ ] **dbt Implementation** - Refactor SQL into dbt models with testing and documentation
- [ ] **Incremental Loads** - Add date-based incremental processing for STAGING layer
- [ ] **Data Quality Tests** - Implement Great Expectations or dbt tests
- [ ] **Orchestration** - Add Airflow DAGs for scheduled execution
- [ ] **Advanced Analytics** - Customer cohort analysis, product recommendation logic
- [ ] **Machine Learning** - Churn prediction model, CLV forecasting
- [ ] **Real-Time Layer** - Stream processing with Kafka + Snowpipe
- [ ] **Dashboard Development** - Build Tableau/Power BI dashboards

---

## License

This project is open source and available under the [MIT License](LICENSE).

---

## Acknowledgments

- **Snowflake** for providing an excellent cloud data warehouse platform
- **Kimball Group** for dimensional modeling methodology

---

**If you found this project helpful, please give it a star!**
