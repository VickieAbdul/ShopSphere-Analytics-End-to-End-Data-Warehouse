"""
ShopSphere Analytics - Visualization Generator
==============================================

This script connects to Snowflake and generates professional visualizations
from the MART layer tables to answer the CEO's 5 key questions.

Requirements:
- snowflake-connector-python
- pandas
- matplotlib
- seaborn
- plotly

Install with: pip install snowflake-connector-python pandas matplotlib seaborn plotly kaleido
"""

import snowflake.connector
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import os

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 10

# Create output directory
os.makedirs('charts', exist_ok=True)

# ============================================================================
# SNOWFLAKE CONNECTION SETUP
# ============================================================================

def connect_to_snowflake():
    """
    Connect to Snowflake. Update these credentials with your own.
    
    SECURITY NOTE: In production, use environment variables or secret management
    Never commit credentials to GitHub!
    """
    conn = snowflake.connector.connect(
        user='YOUR_USERNAME',           # Replace with your username
        password='YOUR_PASSWORD',       # Replace with your password
        account='YOUR_ACCOUNT',         # Replace with your account (e.g., xy12345.us-east-1)
        warehouse='COMPUTE_WH',         # Your warehouse name
        database='SHOPSPHERE_DB',
        schema='SCHEMA_MART'
    )
    return conn

# ============================================================================
# DATA EXTRACTION FUNCTIONS
# ============================================================================

def query_snowflake(conn, query):
    """Execute query and return pandas DataFrame"""
    cursor = conn.cursor()
    cursor.execute(query)
    df = cursor.fetch_pandas_all()
    cursor.close()
    return df

def get_regional_performance(conn):
    """Get regional profitability data"""
    query = """
    SELECT 
        region,
        net_revenue,
        gross_profit,
        overall_profit_margin_pct,
        total_orders,
        unique_customers
    FROM SCHEMA_MART.MART_REGIONAL_PERFORMANCE
    ORDER BY gross_profit DESC
    """
    return query_snowflake(conn, query)

def get_product_performance(conn):
    """Get product category performance data"""
    query = """
    SELECT 
        product_category,
        net_revenue,
        total_units_sold,
        revenue_contribution_pct,
        volume_contribution_pct,
        avg_profit_margin_pct
    FROM SCHEMA_MART.MART_PRODUCT_PERFORMANCE
    ORDER BY net_revenue DESC
    """
    return query_snowflake(conn, query)

def get_discount_impact(conn):
    """Get discount impact analysis data"""
    query = """
    SELECT 
        discount_bucket,
        total_orders,
        net_revenue,
        gross_profit,
        avg_profit_margin_pct,
        avg_order_value
    FROM SCHEMA_MART.MART_DISCOUNT_IMPACT
    ORDER BY 
        CASE discount_bucket
            WHEN 'No Discount' THEN 1
            WHEN '1-10% Discount' THEN 2
            WHEN '11-20% Discount' THEN 3
            WHEN '21-30% Discount' THEN 4
            ELSE 5
        END
    """
    return query_snowflake(conn, query)

def get_customer_segments(conn):
    """Get customer segment performance data"""
    query = """
    SELECT 
        customer_segment,
        total_customers,
        avg_clv_per_customer,
        clv_contribution_pct,
        avg_orders_per_customer,
        total_clv
    FROM SCHEMA_MART.MART_CUSTOMER_SEGMENTS
    ORDER BY total_clv DESC
    """
    return query_snowflake(conn, query)

def get_churn_metrics(conn):
    """Get churn risk distribution data"""
    query = """
    SELECT 
        churn_risk,
        total_customers,
        pct_of_customer_base,
        total_clv_at_risk,
        premium_customers,
        regular_customers,
        budget_customers
    FROM SCHEMA_MART.MART_RETENTION_METRICS
    ORDER BY 
        CASE churn_risk
            WHEN 'High Risk' THEN 1
            WHEN 'Medium Risk' THEN 2
            ELSE 3
        END
    """
    return query_snowflake(conn, query)

# ============================================================================
# VISUALIZATION FUNCTIONS
# ============================================================================

def viz_regional_performance(df):
    """
    CEO Question 1: Are we actually profitable per region?
    Creates a dual-axis chart showing revenue and profit margin by region
    """
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=('Regional Revenue & Profit', 'Profit Margins by Region'),
        specs=[[{"secondary_y": False}, {"type": "bar"}]]
    )
    
    # Chart 1: Revenue and Profit bars
    fig.add_trace(
        go.Bar(name='Net Revenue', x=df['REGION'], y=df['NET_REVENUE'],
               marker_color='#3498db', text=df['NET_REVENUE'].apply(lambda x: f'${x/1e6:.1f}M'),
               textposition='outside'),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Bar(name='Gross Profit', x=df['REGION'], y=df['GROSS_PROFIT'],
               marker_color='#2ecc71', text=df['GROSS_PROFIT'].apply(lambda x: f'${x/1e6:.1f}M'),
               textposition='outside'),
        row=1, col=1
    )
    
    # Chart 2: Profit margins
    fig.add_trace(
        go.Bar(name='Profit Margin %', x=df['REGION'], y=df['OVERALL_PROFIT_MARGIN_PCT'],
               marker_color='#e74c3c', text=df['OVERALL_PROFIT_MARGIN_PCT'].apply(lambda x: f'{x:.1f}%'),
               textposition='outside', showlegend=False),
        row=1, col=2
    )
    
    fig.update_layout(
        title_text="Regional Profitability Analysis",
        height=500,
        showlegend=True,
        font=dict(size=12)
    )
    
    fig.update_xaxes(title_text="Region", row=1, col=1)
    fig.update_xaxes(title_text="Region", row=1, col=2)
    fig.update_yaxes(title_text="Amount ($)", row=1, col=1)
    fig.update_yaxes(title_text="Profit Margin (%)", row=1, col=2)
    
    fig.write_html('charts/regional_performance.html')
    fig.write_image('charts/regional_performance.png', width=1400, height=500)
    print("✅ Created: regional_performance.png")
    
    return fig

def viz_product_performance(df):
    """
    CEO Question 2: Which product categories drive revenue vs volume?
    Creates a scatter plot showing revenue vs volume contribution
    """
    fig = go.Figure()
    
    # Scatter plot with bubble size = profit margin
    fig.add_trace(go.Scatter(
        x=df['VOLUME_CONTRIBUTION_PCT'],
        y=df['REVENUE_CONTRIBUTION_PCT'],
        mode='markers+text',
        marker=dict(
            size=df['AVG_PROFIT_MARGIN_PCT']*2,  # Scale for visibility
            color=df['AVG_PROFIT_MARGIN_PCT'],
            colorscale='Viridis',
            showscale=True,
            colorbar=dict(title="Profit<br>Margin %"),
            line=dict(width=2, color='white')
        ),
        text=df['PRODUCT_CATEGORY'],
        textposition="top center",
        textfont=dict(size=12, color='black'),
        hovertemplate='<b>%{text}</b><br>' +
                      'Volume: %{x:.1f}%<br>' +
                      'Revenue: %{y:.1f}%<br>' +
                      '<extra></extra>'
    ))
    
    # Add diagonal reference line (where revenue % = volume %)
    fig.add_trace(go.Scatter(
        x=[0, 40],
        y=[0, 40],
        mode='lines',
        line=dict(color='gray', dash='dash'),
        showlegend=False,
        hoverinfo='skip'
    ))
    
    fig.update_layout(
        title="Product Performance: Revenue vs Volume Contribution<br>" +
              "<sub>Bubble size = Profit Margin | Above line = Revenue-driven | Below line = Volume-driven</sub>",
        xaxis_title="Volume Contribution (%)",
        yaxis_title="Revenue Contribution (%)",
        height=600,
        width=900,
        showlegend=False,
        font=dict(size=12)
    )
    
    fig.write_html('charts/product_performance.html')
    fig.write_image('charts/product_performance.png', width=900, height=600)
    print("✅ Created: product_performance.png")
    
    return fig

def viz_discount_impact(df):
    """
    CEO Question 3: Is discounting hurting margins?
    Creates a combo chart showing orders vs margins across discount levels
    """
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Bar chart for number of orders
    fig.add_trace(
        go.Bar(
            name='Total Orders',
            x=df['DISCOUNT_BUCKET'],
            y=df['TOTAL_ORDERS'],
            marker_color='#3498db',
            yaxis='y',
            text=df['TOTAL_ORDERS'],
            textposition='outside'
        ),
        secondary_y=False
    )
    
    # Line chart for profit margin
    fig.add_trace(
        go.Scatter(
            name='Avg Profit Margin %',
            x=df['DISCOUNT_BUCKET'],
            y=df['AVG_PROFIT_MARGIN_PCT'],
            mode='lines+markers+text',
            line=dict(color='#e74c3c', width=3),
            marker=dict(size=10),
            text=df['AVG_PROFIT_MARGIN_PCT'].apply(lambda x: f'{x:.1f}%'),
            textposition='top center',
            yaxis='y2'
        ),
        secondary_y=True
    )
    
    fig.update_layout(
        title_text="Discount Impact on Order Volume and Profitability",
        height=500,
        showlegend=True,
        legend=dict(x=0.7, y=1.1, orientation='h'),
        font=dict(size=12)
    )
    
    fig.update_xaxes(title_text="Discount Level")
    fig.update_yaxes(title_text="Number of Orders", secondary_y=False)
    fig.update_yaxes(title_text="Profit Margin (%)", secondary_y=True)
    
    fig.write_html('charts/discount_impact.html')
    fig.write_image('charts/discount_impact.png', width=1000, height=500)
    print("✅ Created: discount_impact.png")
    
    return fig

def viz_customer_segments(df):
    """
    CEO Question 4: Which customer segments are most valuable?
    Creates a stacked view showing segment size vs value contribution
    """
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=('Customer Count by Segment', 'CLV Contribution by Segment'),
        specs=[[{"type": "pie"}, {"type": "pie"}]]
    )
    
    # Pie 1: Customer count
    fig.add_trace(
        go.Pie(
            labels=df['CUSTOMER_SEGMENT'],
            values=df['TOTAL_CUSTOMERS'],
            hole=0.4,
            marker=dict(colors=['#2ecc71', '#3498db', '#95a5a6']),
            textinfo='label+percent',
            textposition='outside'
        ),
        row=1, col=1
    )
    
    # Pie 2: CLV contribution
    fig.add_trace(
        go.Pie(
            labels=df['CUSTOMER_SEGMENT'],
            values=df['CLV_CONTRIBUTION_PCT'],
            hole=0.4,
            marker=dict(colors=['#2ecc71', '#3498db', '#95a5a6']),
            textinfo='label+percent',
            textposition='outside'
        ),
        row=1, col=2
    )
    
    fig.update_layout(
        title_text="Customer Segmentation: Size vs Value<br>" +
                   "<sub>Premium customers are 22% of base but drive 45% of value</sub>",
        height=500,
        showlegend=False,
        font=dict(size=12)
    )
    
    fig.write_html('charts/customer_segments.html')
    fig.write_image('charts/customer_segments.png', width=1200, height=500)
    print("✅ Created: customer_segments.png")
    
    return fig

def viz_churn_metrics(df):
    """
    CEO Question 5: How can we reduce churn?
    Creates a risk distribution chart with segment breakdown
    """
    fig = go.Figure()
    
    # Stacked bar for segment breakdown within each risk level
    fig.add_trace(go.Bar(
        name='Premium',
        x=df['CHURN_RISK'],
        y=df['PREMIUM_CUSTOMERS'],
        marker_color='#2ecc71',
        text=df['PREMIUM_CUSTOMERS'],
        textposition='inside'
    ))
    
    fig.add_trace(go.Bar(
        name='Regular',
        x=df['CHURN_RISK'],
        y=df['REGULAR_CUSTOMERS'],
        marker_color='#3498db',
        text=df['REGULAR_CUSTOMERS'],
        textposition='inside'
    ))
    
    fig.add_trace(go.Bar(
        name='Budget',
        x=df['CHURN_RISK'],
        y=df['BUDGET_CUSTOMERS'],
        marker_color='#95a5a6',
        text=df['BUDGET_CUSTOMERS'],
        textposition='inside'
    ))
    
    fig.update_layout(
        title="Churn Risk Distribution by Customer Segment<br>" +
              "<sub>98.8% of customers are low-risk (active)</sub>",
        xaxis_title="Churn Risk Level",
        yaxis_title="Number of Customers",
        barmode='stack',
        height=500,
        showlegend=True,
        legend=dict(x=0.75, y=1.1, orientation='h'),
        font=dict(size=12)
    )
    
    fig.write_html('charts/churn_risk.html')
    fig.write_image('charts/churn_risk.png', width=900, height=500)
    print("✅ Created: churn_risk.png")
    
    return fig

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """
    Main function to generate all visualizations
    """
    print("\n" + "="*60)
    print("ShopSphere Analytics - Visualization Generator")
    print("="*60 + "\n")
    
    # Connect to Snowflake
    print("🔌 Connecting to Snowflake...")
    try:
        conn = connect_to_snowflake()
        print("✅ Connected successfully!\n")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("\n⚠️  Please update the connection credentials in the script.")
        return
    
    try:
        # Generate all visualizations
        print("📊 Generating visualizations...\n")
        
        print("1️⃣  CEO Question 1: Regional Profitability")
        df_regional = get_regional_performance(conn)
        viz_regional_performance(df_regional)
        
        print("\n2️⃣  CEO Question 2: Product Performance")
        df_product = get_product_performance(conn)
        viz_product_performance(df_product)
        
        print("\n3️⃣  CEO Question 3: Discount Impact")
        df_discount = get_discount_impact(conn)
        viz_discount_impact(df_discount)
        
        print("\n4️⃣  CEO Question 4: Customer Segments")
        df_segments = get_customer_segments(conn)
        viz_customer_segments(df_segments)
        
        print("\n5️⃣  CEO Question 5: Churn Risk")
        df_churn = get_churn_metrics(conn)
        viz_churn_metrics(df_churn)
        
        print("\n" + "="*60)
        print("✅ All visualizations generated successfully!")
        print("📁 Files saved in ./charts/ directory")
        print("="*60 + "\n")
        
    except Exception as e:
        print(f"\n❌ Error generating visualizations: {e}")
    
    finally:
        # Close connection
        conn.close()
        print("🔒 Connection closed.\n")

if __name__ == "__main__":
    main()
