"""Streamlit dashboard for real-time order analytics."""

import os
import pandas as pd
import streamlit as st
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

from demo_data import demo_product_metrics, demo_window_metrics

# Configuration
MONGODB_URI = os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
DATABASE_NAME = os.environ.get("MONGODB_DATABASE", "order_analytics")
COLLECTION_NAME = os.environ.get("MONGODB_COLLECTION", "sales_metrics")
APP_MODE = os.environ.get("APP_MODE", "live").lower()

# Page config
st.set_page_config(page_title="Real-Time Order Analytics", page_icon="📈", layout="wide")

# Custom CSS for modern dark UI
st.markdown("""
<style>
    /* Metric Card Styling */
    [data-testid="stMetric"] {
        background-color: #1E293B;
        border: 1px solid #334155;
        border-radius: 8px;
        padding: 15px 20px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
    }
    
    /* Subtle typography tweaks */
    h1, h2, h3 {
        font-weight: 600 !important;
        letter-spacing: -0.025em;
    }
    
    /* Status Badge */
    .status-badge {
        display: inline-block;
        padding: 4px 10px;
        border-radius: 4px;
        font-size: 12px;
        font-weight: bold;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-left: 10px;
        vertical-align: middle;
    }
    .status-demo {
        background-color: #334155;
        color: #06B6D4;
        border: 1px solid #06B6D4;
    }
    .status-live {
        background-color: #14532d;
        color: #4ade80;
        border: 1px solid #4ade80;
    }
    
    /* Architecture tags */
    .arch-tag {
        display: inline-block;
        background-color: #334155;
        color: #94A3B8;
        padding: 4px 12px;
        border-radius: 16px;
        font-size: 13px;
        margin-right: 8px;
        margin-bottom: 8px;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_mongo_client():
    """Return a MongoDB client."""
    return MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)

def fetch_window_metrics(limit=60):
    """Fetch the latest window metrics from MongoDB for the trend chart and KPIs."""
    if APP_MODE == "demo":
        return demo_window_metrics()
    try:
        client = get_mongo_client()
        collection = client[DATABASE_NAME][COLLECTION_NAME]
        
        cursor = collection.find({"metric_type": "window"}).sort("window_start", -1).limit(limit)
        data = list(cursor)
        
        if not data:
            return pd.DataFrame()
            
        flat_data = []
        for doc in data:
            flat_data.append({
                "window_start": doc.get("window_start"),
                "window_end": doc.get("window_end"),
                "total_sales": doc.get("total_sales", 0),
                "order_count": doc.get("order_count", 0),
                "total_quantity": doc.get("total_quantity", 0)
            })
            
        return pd.DataFrame(flat_data)
    except (ConnectionFailure, ServerSelectionTimeoutError):
        st.error("Analytics storage is temporarily unavailable. Please refresh in a moment.")
        return pd.DataFrame()

def fetch_top_products_all_time():
    """Fetch all-time top products using MongoDB aggregation."""
    if APP_MODE == "demo":
        return demo_product_metrics()
    try:
        client = get_mongo_client()
        collection = client[DATABASE_NAME][COLLECTION_NAME]
        
        pipeline = [
            {"$match": {"metric_type": "product"}},
            {"$group": {
                "_id": "$product",
                "total_sales": {"$sum": "$total_sales"},
                "order_count": {"$sum": "$order_count"},
                "total_quantity": {"$sum": "$total_quantity"}
            }},
            {"$sort": {"total_sales": -1}}
        ]
        
        data = list(collection.aggregate(pipeline))
        if not data:
            return pd.DataFrame()
            
        flat_data = []
        for doc in data:
            product_name = doc.get("_id")
            if not product_name:
                product_name = "Unknown"
                
            flat_data.append({
                "product": product_name,
                "total_sales": doc.get("total_sales", 0),
                "order_count": doc.get("order_count", 0),
                "total_quantity": doc.get("total_quantity", 0)
            })
            
        return pd.DataFrame(flat_data)
    except (ConnectionFailure, ServerSelectionTimeoutError):
        st.error("Analytics storage is temporarily unavailable. Please refresh in a moment.")
        return pd.DataFrame()


# Header Section
st.markdown(f"""
    <h1>
        Real-Time Order Analytics 
        <span class="status-badge {'status-demo' if APP_MODE == 'demo' else 'status-live'}">
            {'DEMO MODE' if APP_MODE == 'demo' else 'LIVE PIPELINE'}
        </span>
    </h1>
    <p style="color: #94A3B8; font-size: 1.1rem; margin-bottom: 0;">
        E-commerce streaming pipeline powered by Apache Kafka, PySpark, and MongoDB.
    </p>
""", unsafe_allow_html=True)

# Architecture Section
st.markdown("""
    <div style="margin-top: 15px; margin-bottom: 25px;">
        <span class="arch-tag">Producer</span> ➔ 
        <span class="arch-tag">Kafka</span> ➔ 
        <span class="arch-tag">PySpark Structured Streaming</span> ➔ 
        <span class="arch-tag">MongoDB & MinIO</span>
    </div>
""", unsafe_allow_html=True)


if APP_MODE == "demo":
    st.info(
        "**Portfolio Demo Note:** You are viewing deterministic sample data to demonstrate the dashboard UI at zero cost. "
        "The live Kafka/Spark backend runs exclusively in the local Docker environment."
    )

col1, col2 = st.columns([8, 1])
with col2:
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.rerun()

# Fetch Data
df_window = fetch_window_metrics()
df_products = fetch_top_products_all_time()

if df_window.empty:
    st.warning("No data available yet. Ensure the pipeline is running and processing events.")
else:
    df_window["window_start"] = pd.to_datetime(df_window["window_start"])
    df_window = df_window.sort_values("window_start")
    
    latest_window_row = df_window.iloc[-1]
    
    total_sales = latest_window_row["total_sales"]
    total_orders = latest_window_row["order_count"]
    avg_order_value = total_sales / total_orders if total_orders > 0 else 0
    
    # Calculate top selling product overall (or we can just show total pipeline sales if we sum up df_window)
    # Let's show pipeline total sales from df_products
    overall_total_sales = df_products["total_sales"].sum() if not df_products.empty else total_sales
    
    st.markdown("### Executive Summary")
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.metric("Current Window Sales", f"₹{total_sales:,.2f}")
    kpi2.metric("Current Window Orders", int(total_orders))
    kpi3.metric("Avg Order Value", f"₹{avg_order_value:,.2f}")
    kpi4.metric("Pipeline Total Sales", f"₹{overall_total_sales:,.2f}")
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        st.markdown("### Sales Trend Over Time")
        chart_data = df_window.set_index("window_start")["total_sales"]
        st.area_chart(chart_data, color="#06B6D4")
        
    with col_chart2:
        st.markdown("### Top Products (All-Time Revenue)")
        if not df_products.empty:
            product_chart_data = df_products.set_index("product")["total_sales"]
            st.bar_chart(product_chart_data, color="#3b82f6")
        else:
            st.info("No product data available.")
        
    st.markdown("---")
    st.markdown("### Recent Aggregation Windows")
    st.dataframe(
        df_window.sort_values("window_start", ascending=False),
        use_container_width=True,
        hide_index=True
    )
