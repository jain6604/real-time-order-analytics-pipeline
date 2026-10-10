"""Streamlit dashboard for real-time order analytics."""

import os
import pandas as pd
import streamlit as st
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

from demo_data import generate_sample_orders

# Configuration
MONGODB_URI = os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
DATABASE_NAME = os.environ.get("MONGODB_DATABASE", "order_analytics")
COLLECTION_NAME = os.environ.get("MONGODB_COLLECTION", "sales_metrics")
APP_MODE = os.environ.get("APP_MODE", "live").lower()

# Page config
st.set_page_config(page_title="Sales Analytics Pipeline", page_icon="📈", layout="wide")

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
    
    h1, h2, h3 {
        font-weight: 600 !important;
        letter-spacing: -0.025em;
    }
    
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
    .status-demo { background-color: #334155; color: #06B6D4; border: 1px solid #06B6D4; }
    .status-live { background-color: #14532d; color: #4ade80; border: 1px solid #4ade80; }
    
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
    return MongoClient(MONGODB_URI, serverSelectionTimeoutMS=2000)

def fetch_live_metrics():
    """Fetch window, product, and city metrics from MongoDB."""
    try:
        client = get_mongo_client()
        collection = client[DATABASE_NAME][COLLECTION_NAME]
        
        # Window Metrics
        window_cursor = collection.find({"metric_type": "window"}).sort("window_start", -1).limit(60)
        df_window = pd.DataFrame(list(window_cursor))
        
        # Product Metrics (All-time aggregate)
        prod_pipeline = [
            {"$match": {"metric_type": "product"}},
            {"$group": {"_id": "$product", "total_sales": {"$sum": "$total_sales"}, "total_quantity": {"$sum": "$total_quantity"}}}
        ]
        df_products = pd.DataFrame(list(collection.aggregate(prod_pipeline)))
        if not df_products.empty:
            df_products = df_products.rename(columns={"_id": "product"})
            
        # City Metrics (All-time aggregate)
        city_pipeline = [
            {"$match": {"metric_type": "city"}},
            {"$group": {"_id": "$city", "total_sales": {"$sum": "$total_sales"}}}
        ]
        df_cities = pd.DataFrame(list(collection.aggregate(city_pipeline)))
        if not df_cities.empty:
            df_cities = df_cities.rename(columns={"_id": "city"})
            
        return df_window, df_products, df_cities
    except (ConnectionFailure, ServerSelectionTimeoutError):
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


def main():
    APP_MODE = os.environ.get("APP_MODE", "live").lower()

    # Page config
    st.set_page_config(page_title="Sales Analytics Pipeline", page_icon="📈", layout="wide")

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
        
        h1, h2, h3 {
            font-weight: 600 !important;
            letter-spacing: -0.025em;
        }
        
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
        .status-demo { background-color: #334155; color: #06B6D4; border: 1px solid #06B6D4; }
        .status-live { background-color: #14532d; color: #4ade80; border: 1px solid #4ade80; }
        
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

    # ---------------- Header ----------------
    st.markdown(f"""
        <h1>
            E-Commerce Sales Analytics 
            <span class="status-badge {'status-demo' if APP_MODE == 'demo' else 'status-live'}">
                {'DEMO MODE' if APP_MODE == 'demo' else 'LIVE PIPELINE'}
            </span>
        </h1>
        <p style="color: #94A3B8; font-size: 1.1rem; margin-bottom: 5px;">
            Real-time streaming pipeline processing order events.
        </p>
    """, unsafe_allow_html=True)

    with st.expander("How the Pipeline Works"):
        st.markdown("""
        <div style="margin-top: 5px; margin-bottom: 15px;">
            <span class="arch-tag">Python Generator</span> ➔ 
            <span class="arch-tag">Apache Kafka</span> ➔ 
            <span class="arch-tag">PySpark Streaming</span> ➔ 
            <span class="arch-tag">MongoDB (Analytics) & MinIO (Raw)</span>
        </div>
        
        1. **Python Order Generator** simulates high-throughput e-commerce transactions.
        2. **Apache Kafka (KRaft)** ingests the event stream durably.
        3. **PySpark Structured Streaming** processes events, calculating 5-minute tumbling window metrics.
        4. **MinIO** safely archives all raw events as Parquet files for the Data Lake.
        5. **MongoDB** stores the low-latency aggregated metrics powering this dashboard.
        """, unsafe_allow_html=True)


    # ---------------- Data Loading & Filtering ----------------
    if APP_MODE == "demo":
        st.info("**Portfolio Demo:** You are interacting with simulated data. The live Kafka/Spark backend runs exclusively in the local Docker environment.")
        
        if "demo_orders" not in st.session_state:
            st.session_state["demo_orders"] = generate_sample_orders(200)
            
        col1, col2 = st.columns([8, 2])
        with col2:
            if st.button("🚀 Simulate New Orders", use_container_width=True):
                new_orders = generate_sample_orders(50)
                st.session_state["demo_orders"] = pd.concat([st.session_state["demo_orders"], new_orders], ignore_index=True)
                
        raw_df = st.session_state["demo_orders"].copy()
        
        # Sidebar Filters
        st.sidebar.header("Interactive Analytics")
        cities = st.sidebar.multiselect("Filter by City", options=raw_df["city"].unique())
        products = st.sidebar.multiselect("Filter by Product", options=raw_df["product"].unique())
        
        if cities:
            raw_df = raw_df[raw_df["city"].isin(cities)]
        if products:
            raw_df = raw_df[raw_df["product"].isin(products)]
            
        if raw_df.empty:
            st.warning("No orders match the selected filters.")
            st.stop()
            
        # Compute metrics from filtered raw data
        total_sales = raw_df["sales"].sum()
        total_orders = len(raw_df)
        avg_order_value = total_sales / total_orders if total_orders > 0 else 0
        units_sold = raw_df["quantity"].sum()
        best_product = raw_df.groupby("product")["sales"].sum().idxmax()
        
        # Trend over time (group by 5 minute windows)
        df_trend = raw_df.set_index("timestamp").resample("5min")["sales"].sum().reset_index()
        
        # Product & City aggregates
        df_products = raw_df.groupby("product").agg({"sales": "sum", "quantity": "sum"}).reset_index().rename(columns={"sales": "total_sales", "quantity": "total_quantity"})
        df_cities = raw_df.groupby("city").agg({"sales": "sum"}).reset_index().rename(columns={"sales": "total_sales"})
        
    else:
        # LIVE PIPELINE MODE
        df_window, df_products, df_cities = fetch_live_metrics()
        
        if df_window.empty:
            st.error("Live MongoDB backend is unreachable or contains no data. Please run `docker compose up -d`.")
            st.stop()
            
        df_window["window_start"] = pd.to_datetime(df_window["window_start"])
        df_window = df_window.sort_values("window_start")
        
        total_sales = df_products["total_sales"].sum() if not df_products.empty else 0
        total_orders = df_window["order_count"].sum()
        avg_order_value = total_sales / total_orders if total_orders > 0 else 0
        units_sold = df_products["total_quantity"].sum() if not df_products.empty else 0
        best_product = df_products.loc[df_products["total_sales"].idxmax(), "product"] if not df_products.empty else "N/A"
        
        df_trend = df_window.rename(columns={"window_start": "timestamp", "total_sales": "sales"})
        raw_df = pd.DataFrame() # No raw data in live mode dashboard


    # ---------------- KPI Cards ----------------
    st.markdown("### Business KPIs")
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Total Revenue", f"₹{total_sales:,.0f}")
    k2.metric("Total Orders", f"{total_orders:,}")
    k3.metric("Avg Order Value", f"₹{avg_order_value:,.0f}")
    k4.metric("Units Sold", f"{units_sold:,}")
    k5.metric("Top Product", best_product)

    st.markdown("<br>", unsafe_allow_html=True)


    # ---------------- Charts ----------------
    c1, c2 = st.columns([2, 1])

    with c1:
        st.markdown("### Revenue Trend")
        st.area_chart(df_trend.set_index("timestamp")["sales"], color="#06B6D4")

    with c2:
        st.markdown("### Revenue by City")
        if not df_cities.empty:
            st.bar_chart(df_cities.set_index("city")["total_sales"], color="#3b82f6")
        else:
            st.info("City metrics not available.")

    c3, c4 = st.columns(2)
    with c3:
        st.markdown("### Top Products (Revenue)")
        if not df_products.empty:
            st.bar_chart(df_products.sort_values("total_sales", ascending=False).set_index("product")["total_sales"], color="#0ea5e9")

    with c4:
        st.markdown("### Top Products (Units Sold)")
        if not df_products.empty:
            st.bar_chart(df_products.sort_values("total_quantity", ascending=False).set_index("product")["total_quantity"], color="#14b8a6")


    # ---------------- Recent Orders Table ----------------
    st.markdown("---")
    st.markdown("### Recent Order Transactions")

    if APP_MODE == "demo":
        st.dataframe(
            raw_df.sort_values("timestamp", ascending=False).head(100),
            use_container_width=True,
            hide_index=True
        )
        
        # CSV Export
        csv = raw_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Download Filtered Data as CSV",
            data=csv,
            file_name='ecommerce_orders.csv',
            mime='text/csv',
        )
    else:
        st.info("ℹ️ **Data Lake Architecture:** In Live Pipeline mode, granular raw order data is securely archived in the **MinIO S3-compatible Data Lake** as partitioned Parquet files. This dashboard is powered by the real-time aggregations stored in MongoDB for ultra-low latency.")

if __name__ == "__main__":
    main()
