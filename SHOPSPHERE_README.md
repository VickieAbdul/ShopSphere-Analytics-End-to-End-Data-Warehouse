# ShopSphere Analytics: End-to-End Data Warehouse for E-Commerce Intelligence

## Project Overview
This project builds a complete analytics engineering pipeline for ShopSphere, a fictional global e-commerce company operating across 8 countries. The goal was to move from raw, unstructured transaction data to a clean, structured data warehouse that could answer real executive questions about profitability, product performance, discounting, customer value, and churn.

The project simulates what an analytics engineer would actually build in a production environment, from raw data ingestion all the way through to executive ready KPI tables.

---

## Business Problem
ShopSphere had recently migrated to Snowflake but had no structured analytics layer. Leadership could not answer five questions that any e-commerce CEO needs to run the business:

1. Are we actually profitable per region?
2. Which product categories drive revenue versus just volume?
3. Is our discounting strategy hurting margins?
4. Which customer segments are most valuable?
5. How do we reduce churn?

---

## Dataset Description
The dataset covers ShopSphere's global operations including:

- 20,000+ sales transactions across 8 countries and 5 regions
- 500 customers with purchase history and segmentation data
- 200 products across Electronics, Fashion, Home, Beauty, and Sports categories
- Financial fields including revenue, cost, discount rates, and profit margins

---

## Tools Used
- Snowflake (cloud data warehouse)
- SQL (Snowflake dialect)
- Python (Pandas, Matplotlib, Plotly)

---

## Analysis Structure

### 1. Data Architecture: 5-Layer Warehouse Design
Built a production grade pipeline with five distinct layers: Raw, Staging, Clean, Analytics, and Mart. Each layer has a specific responsibility, from preserving the original source data all the way through to pre-aggregated tables optimized for dashboard consumption.

### 2. Data Quality & Cleaning
Before any analysis, the data went through rigorous quality checks:
- Removed 386 duplicate order IDs
- Removed 197 invalid transactions with negative quantities
- Replaced 584 NULL discount values with zero
- Result: 19,422 clean, reliable transactions ready for analysis

### 3. Dimensional Modeling
Built a star schema following Kimball methodology with a central FACT_SALES table joined to customer, product, and date dimensions. Revenue, profit, and margin were calculated at the transaction level to enable flexible aggregation at any grain.

### 4. Customer Segmentation (RFM Analysis)
Segmented the customer base by recency, frequency, and monetary value to classify customers into Premium, Regular, and Budget tiers and calculate Customer Lifetime Value and churn risk per segment.

### 5. Mart Layer: Answering the CEO's 5 Questions
Built five dedicated mart tables, one per business question, pre-aggregated and optimized for BI tools so dashboards load fast and analysts do not need to rewrite complex queries every time.

---

## Key Insights

- Total revenue across 19,422 transactions was $55.26M with a gross profit of $18.74M and an average margin of 39.43%, which is strong for e-commerce.
- All five regions are profitable. North America leads in total revenue while the Middle East carries the highest profit margins, ranging between 35% and 42% across the portfolio.
- Electronics drives 35% of revenue with the highest margins despite lower volume. Fashion drives 25% of revenue through volume but at lower margins. The business is more dependent on Electronics profitability than the revenue split alone suggests.
- Discounting is not hurting the business. Even at 21 to 30% discount levels, margins stay above 35%. Non-discounted orders average 41% margin versus 35% for discounted ones, a gap that is manageable and does not justify pulling back on promotions.
- 22% of customers, just 110 people, generate 45% of total Customer Lifetime Value. The Pareto principle holds firmly here.
- Churn is not a crisis. 98.8% of customers are low risk and zero customers fall into the high risk category. The bigger opportunity is retention and growth of the Premium segment, not broad churn prevention.

---

## Recommendations

- Protect and invest in the Premium customer segment. 110 customers generating 45% of CLV means that losing even a handful of them has an outsized revenue impact. They deserve a dedicated retention and loyalty strategy.
- Prioritise Electronics in marketing and inventory planning. It delivers the best margin per transaction. Volume focused strategies that push Fashion at the expense of Electronics margin are working against the profit structure.
- Keep the current discounting strategy. The data does not support cutting back on discounts. Margins hold well even at higher discount levels and the revenue volume they generate is worth the modest margin difference.
- Do not over-invest in churn prevention right now. With 98.8% of customers in the low risk category, broad churn campaigns would waste budget. Focus retention efforts narrowly on the 6 medium risk customers and monitor the Premium segment closely.
- Build on this foundation. The warehouse is built. The next step is connecting it to a live BI tool so leadership can monitor these KPIs in real time rather than through static queries.

---

## Executive Conclusion
ShopSphere is a profitable, well-distributed business with a clear revenue structure and a healthy customer base. The data warehouse built in this project gives leadership the visibility they were missing, and the insights it surfaces are actionable. The business should double down on Electronics margins, protect its Premium customers, and feel confident that its discounting strategy is not doing the damage that was feared.

The infrastructure is now in place for ShopSphere to make faster, more confident decisions at every level of the organisation.

## Contact

**Victoria Abdul**  
Data Analyst | Snowflake | Analytics Engineering

- Email: victoria.j.abdulkadir@gmail.com
- LinkedIn: [linkedin.com/in/victoria-abdul](https://www.linkedin.com/in/victoriajabdul/)
- GitHub: [github.com/vickieabdul](https://github.com/vickieabdul)


---

## License

This project is open source and available under the [MIT License](LICENSE).

---

## Acknowledgments

- **Snowflake** for providing an excellent cloud data warehouse platform
- **Kimball Group** for dimensional modeling methodology
- **dbt Labs** for modern analytics engineering principles

---

**⭐ If you found this project helpful, please give it a star!**
