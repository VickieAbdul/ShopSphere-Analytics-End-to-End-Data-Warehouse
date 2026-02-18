# Visualization Setup Guide

## Quick Start

### Step 1: Install Python Dependencies

```bash
pip install -r requirements.txt
```

### Step 2: Update Snowflake Credentials

Open `generate_visualizations.py` and update lines 37-42 with your Snowflake credentials:

```python
conn = snowflake.connector.connect(
    user='YOUR_USERNAME',           # Your Snowflake username
    password='YOUR_PASSWORD',       # Your Snowflake password
    account='YOUR_ACCOUNT',         # e.g., xy12345.us-east-1
    warehouse='COMPUTE_WH',         # Your warehouse name
    database='SHOPSPHERE_DB',
    schema='SCHEMA_MART'
)
```

**Security Note:** Never commit credentials to GitHub! Use environment variables in production:

```python
import os

conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse='COMPUTE_WH',
    database='SHOPSPHERE_DB',
    schema='SCHEMA_MART'
)
```

### Step 3: Run the Script

```bash
python generate_visualizations.py
```

### Step 4: View Results

All visualizations will be saved in the `charts/` directory:
- `regional_performance.png` - Regional profitability analysis
- `product_performance.png` - Revenue vs volume by category
- `discount_impact.png` - Discount strategy effectiveness
- `customer_segments.png` - Customer segment distribution
- `churn_risk.png` - Churn risk analysis

Interactive HTML versions are also created for exploration!

---

## What Each Visualization Shows

### 1. Regional Performance (`regional_performance.png`)
**Answers:** "Are we actually profitable per region?"
- **Left chart:** Revenue vs Profit bars by region
- **Right chart:** Profit margins by region
- **Key insight:** All regions profitable, identify strongest performers

### 2. Product Performance (`product_performance.png`)
**Answers:** "Which categories drive revenue vs volume?"
- **Scatter plot:** X-axis = volume %, Y-axis = revenue %
- **Bubble size:** Profit margin
- **Key insight:** Categories above the diagonal are revenue-driven (high-value)

### 3. Discount Impact (`discount_impact.png`)
**Answers:** "Is discounting hurting margins?"
- **Bars:** Order volume at each discount level
- **Line:** Profit margin trend
- **Key insight:** Shows if heavy discounting erodes profitability

### 4. Customer Segments (`customer_segments.png`)
**Answers:** "Which segments are most valuable?"
- **Left pie:** Customer count distribution
- **Right pie:** Revenue contribution
- **Key insight:** Validates Pareto principle (80/20 rule)

### 5. Churn Risk (`churn_risk.png`)
**Answers:** "How can we reduce churn?"
- **Stacked bars:** Segment breakdown within each risk level
- **Key insight:** Identifies which valuable customers are at risk

---

## Troubleshooting

### Error: "Module not found"
Solution: Make sure you installed all dependencies
```bash
pip install -r requirements.txt
```

### Error: "Connection refused" or "Authentication failed"
Solution: Check your Snowflake credentials in `generate_visualizations.py`

### Error: "Table does not exist"
Solution: Make sure all MART tables are created in Snowflake first:
```sql
SHOW TABLES IN SCHEMA SCHEMA_MART;
```

You should see:
- MART_REGIONAL_PERFORMANCE
- MART_PRODUCT_PERFORMANCE
- MART_DISCOUNT_IMPACT
- MART_CUSTOMER_SEGMENTS
- MART_RETENTION_METRICS

### Charts look blurry or low quality
Solution: Increase the width/height in the `write_image()` calls:
```python
fig.write_image('charts/filename.png', width=1600, height=800)
```

---

## Customization

### Change Colors
Update the `marker_color` parameters in each function:
```python
marker_color='#3498db'  # Blue
marker_color='#2ecc71'  # Green
marker_color='#e74c3c'  # Red
```

### Change Chart Size
Modify the `height` and `width` parameters:
```python
fig.update_layout(height=600, width=1000)
```

### Add More Charts
Create new functions following this template:
```python
def viz_your_analysis(df):
    """Your description"""
    fig = go.Figure()
    
    # Add your traces here
    
    fig.update_layout(
        title="Your Title",
        height=500
    )
    
    fig.write_html('charts/your_chart.html')
    fig.write_image('charts/your_chart.png')
    print("✅ Created: your_chart.png")
    
    return fig
```

---

## Alternative: Manual Chart Creation

If you prefer not to use Python, you can:

1. **Export data from Snowflake**
   ```sql
   SELECT * FROM SCHEMA_MART.MART_REGIONAL_PERFORMANCE;
   ```
   Click "Download results" in Snowsight

2. **Use Excel/Google Sheets**
   - Import the CSV
   - Create charts manually
   - Take screenshots for README

3. **Use Tableau Public (Free)**
   - Connect to Snowflake
   - Drag and drop to create dashboards
   - Publish to Tableau Public

4. **Use Power BI Desktop (Free)**
   - Import from Snowflake
   - Create visualizations
   - Export as images

---

## Next Steps

After generating visualizations:

1. **Update README.md**
   - Add visualization images under each business insight section
   - Use relative paths: `![Regional Performance](charts/regional_performance.png)`

2. **Create a Portfolio Presentation**
   - Slide 1: Project overview
   - Slides 2-6: Each visualization with business insights
   - Slide 7: Technical implementation highlights
   - Slide 8: Key learnings

3. **Record a Demo**
   - Screen record yourself walking through the visualizations
   - Explain the business insights
   - Show the SQL queries behind each chart
   - Upload to YouTube/LinkedIn

---

## Questions?

If you encounter issues, check:
- Snowflake connection is active
- All MART tables exist and have data
- Python dependencies are installed correctly
- File permissions allow writing to `charts/` directory
