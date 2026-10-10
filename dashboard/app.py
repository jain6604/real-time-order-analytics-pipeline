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
st.set_page_config(page_title="Real-Time Order Analytics", page_icon="🛒", layout="wide")

st.empty() # Placeholder for potential auto-refresh logic

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
            # Handle missing or null products
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

st.title("🛒 Real-Time Order Analytics")

if APP_MODE == "demo":
    st.info(
        "**Public portfolio demo:** this page uses a deterministic sample dataset. "
        "Kafka, Spark, MinIO, and MongoDB run together only in the documented local Docker workflow."
    )

# Add a refresh button
col1, col2 = st.columns([8, 1])
with col1:
    st.write(
        "Deterministic sample analytics for the public demo."
        if APP_MODE == "demo"
        else "Live data fetched from MongoDB, updated by PySpark Structured Streaming."
    )
with col2:
    if st.button("🔄 Refresh Data"):
        st.rerun()

df_window = fetch_window_metrics()
df_products = fetch_top_products_all_time()

if df_window.empty:
    st.warning("No data available yet. Ensure the pipeline is running and processing events.")
else:
    # Convert window_start to datetime for sorting and display
    df_window["window_start"] = pd.to_datetime(df_window["window_start"])
    # Sort chronologically for the trend chart
    df_window = df_window.sort_values("window_start")
    
    # KPIs based on the latest window
    latest_window_row = df_window.iloc[-1]
    
    total_sales = latest_window_row["total_sales"]
    total_orders = latest_window_row["order_count"]
    avg_order_value = total_sales / total_orders if total_orders > 0 else 0
    
    st.subheader(f"Latest Window KPIs (Window: {latest_window_row['window_start']})")
    kpi1, kpi2, kpi3 = st.columns(3)
    kpi1.metric("Total Sales", f"₹{total_sales:,.2f}")
    kpi2.metric("Order Count", int(total_orders))
    kpi3.metric("Avg Order Value", f"₹{avg_order_value:,.2f}")
    
    st.markdown("---")
    
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        st.subheader("Sales Trend Over Time")
        # Line chart expects the index to be the x-axis
        chart_data = df_window.set_index("window_start")["total_sales"]
        st.line_chart(chart_data)
        
    with col_chart2:
        st.subheader("Top Products (All-Time in DB)")
        if not df_products.empty:
            product_chart_data = df_products.set_index("product")["total_sales"]
            st.bar_chart(product_chart_data)
        else:
            st.info("No product data available.")
        
    st.markdown("---")
    st.subheader("Raw Analytics Data (Latest Windows)")
    # Show in descending order for the table
    st.dataframe(df_window.sort_values("window_start", ascending=False), use_container_width=True)
