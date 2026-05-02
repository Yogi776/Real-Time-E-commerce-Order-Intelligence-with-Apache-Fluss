"""
Real-Time E-commerce Order Intelligence Dashboard

Production-ready Streamlit dashboard optimized for fast queries:
  - Reads pre-materialized KPIs from dashboard_kpis (single query)
  - Consolidates fact table samples into minimal queries
  - Runs independent queries in parallel (ThreadPoolExecutor)
  - Auto-refreshes every N seconds

Usage:
    pip install -r dashboard/requirements.txt
    streamlit run dashboard/streamlit_app.py
"""

import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import streamlit as st
import pandas as pd
import plotly.express as px

sys.path.insert(0, str(Path(__file__).parent))
from flink_gateway_client import FlinkGatewayClient

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="E-commerce Order Intelligence",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Custom CSS
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    .block-container { padding-top: 1.2rem; padding-bottom: 1rem; }
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, #667eea08, #764ba208);
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 14px 18px;
    }
    [data-testid="stMetric"] label {
        font-size: 0.78rem; font-weight: 600; color: #64748b;
        text-transform: uppercase; letter-spacing: 0.04em;
    }
    [data-testid="stMetric"] [data-testid="stMetricValue"] {
        font-size: 1.7rem; font-weight: 700;
    }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px 8px 0 0; padding: 8px 20px; font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("### Configuration")
    gateway_url = st.text_input("SQL Gateway URL", value="http://localhost:8085")
    refresh_interval = st.selectbox(
        "Auto-refresh (seconds)", [30, 60, 120, 300, 600], index=3
    )
    st.markdown("---")

    st.markdown("### System Health")
    client = FlinkGatewayClient(gateway_url=gateway_url)
    healthy = client.is_healthy()
    if healthy:
        st.success("SQL Gateway: Connected")
    else:
        st.error("SQL Gateway: Unavailable")
        st.info("Start the platform:\n```\ndocker compose up -d\n```")

    st.markdown("---")
    st.markdown(
        "**Apache Fluss** + **Apache Flink**\n\n"
        "Real-time streaming analytics"
    )
    last_refresh_slot = st.empty()

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.markdown("# Real-Time E-commerce Order Intelligence")
st.caption(
    f"Live analytics via Apache Fluss + Flink "
    f"| Refreshes every {refresh_interval}s "
    f"| {datetime.now().strftime('%H:%M:%S')}"
)

if not healthy:
    st.warning(
        "Cannot reach the Flink SQL Gateway. "
        "Ensure Docker services are running."
    )
    st.stop()


# ---------------------------------------------------------------------------
# Parallel query engine — runs all queries concurrently
# ---------------------------------------------------------------------------
QUERIES = {
    "kpis": "SELECT metric_key, metric_value, updated_at FROM dashboard_kpis LIMIT 10",
    "hv_count": "SELECT COUNT(*) AS v FROM high_value_orders",
    "susp_count": "SELECT COUNT(*) AS v FROM suspicious_orders",
    "revenue": "SELECT * FROM revenue_5min LIMIT 200",
    "city_revenue": "SELECT * FROM city_revenue_5min LIMIT 200",
    "orders_sample": (
        "SELECT order_id, customer_name, product_name, category, city, "
        "order_amount, payment_status, payment_method, device_type, "
        "loyalty_tier, event_time "
        "FROM orders_enriched LIMIT 500"
    ),
    "hv_orders": (
        "SELECT order_id, customer_name, loyalty_tier, category, city, "
        "order_amount, event_time FROM high_value_orders LIMIT 30"
    ),
    "susp_orders": (
        "SELECT order_id, customer_id, order_amount, payment_status, "
        "payment_method, device_type, city, reason, event_time "
        "FROM suspicious_orders LIMIT 30"
    ),
    "customers": "SELECT * FROM customer_profile LIMIT 50",
    "products": (
        "SELECT product_id, product_name, category, brand, "
        "unit_price, inventory_count FROM product_catalog LIMIT 50"
    ),
}


def _run_single_query(args):
    """Worker function — each thread gets its own client + session."""
    name, sql, url = args
    try:
        c = FlinkGatewayClient(gateway_url=url, max_wait=30, poll_interval=0.2)
        return name, c.query(sql)
    except Exception:
        return name, pd.DataFrame()


@st.cache_data(ttl=refresh_interval)
def fetch_all_data(_ts: int = 0) -> dict:
    """Execute all dashboard queries in parallel (5 workers), return named DataFrames."""
    tasks = [(n, s, gateway_url) for n, s in QUERIES.items()]
    results = {}

    with ThreadPoolExecutor(max_workers=5) as pool:
        for name, df in pool.map(_run_single_query, tasks):
            results[name] = df

    return results


cache_ts = int(time.time() // refresh_interval)
load_start = time.time()
data = fetch_all_data(_ts=cache_ts)
load_time = time.time() - load_start


def to_numeric_cols(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


# ---------------------------------------------------------------------------
# Extract KPIs from single dashboard_kpis query
# ---------------------------------------------------------------------------
kpis_df = data["kpis"]


def get_kpi(key, default=0.0):
    if kpis_df.empty:
        return default
    row = kpis_df[kpis_df["metric_key"] == key]
    if row.empty:
        return default
    try:
        return float(row.iloc[0]["metric_value"])
    except (ValueError, TypeError):
        return default


def safe_int(df, default=0):
    try:
        return int(df.iloc[0, 0])
    except Exception:
        return default


# ---------------------------------------------------------------------------
# KPI Row
# ---------------------------------------------------------------------------
k1, k2, k3, k4, k5, k6 = st.columns(6)

with k1:
    st.metric("Orders/min", f"{get_kpi('total_orders_1min'):,.0f}")
with k2:
    st.metric("Revenue/min (INR)", f"₹{get_kpi('total_revenue_1min'):,.0f}")
with k3:
    st.metric("Failed/min", f"{get_kpi('failed_payments_1min'):,.0f}")
with k4:
    st.metric("Events/sec", f"{get_kpi('events_per_second'):,.1f}")
with k5:
    st.metric("High-Value Alerts", f"{safe_int(data['hv_count']):,}")
with k6:
    st.metric("Suspicious Orders", f"{safe_int(data['susp_count']):,}")

st.caption(f"⚡ Data loaded in {load_time:.1f}s (10 queries, 5 parallel workers)")
st.markdown("---")

# ---------------------------------------------------------------------------
# Shared data references
# ---------------------------------------------------------------------------
orders_sample = data["orders_sample"]
if not orders_sample.empty:
    orders_sample = to_numeric_cols(orders_sample, ["order_amount"])

rev_data = data["revenue"]
if not rev_data.empty:
    rev_data = to_numeric_cols(rev_data, [
        "total_revenue", "total_orders", "failed_payments", "avg_order_value"
    ])

city_data = data["city_revenue"]
if not city_data.empty:
    city_data = to_numeric_cols(city_data, ["total_revenue", "total_orders"])

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab_revenue, tab_orders, tab_alerts, tab_deep = st.tabs([
    "Revenue Analytics",
    "Live Order Feed",
    "Alerts & Anomalies",
    "Deep Analytics",
])

# ===========================================================================
# TAB 1: REVENUE
# ===========================================================================
with tab_revenue:
    rev_left, rev_right = st.columns(2)

    with rev_left:
        st.markdown("#### Revenue by Category")
        if not rev_data.empty:
            cat_agg = (
                rev_data.groupby("category", as_index=False)
                .agg(revenue=("total_revenue", "sum"), orders=("total_orders", "sum"))
                .sort_values("revenue", ascending=False)
            )
            fig = px.bar(
                cat_agg, x="category", y="revenue", color="category",
                text_auto=".2s",
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            fig.update_layout(
                showlegend=False, xaxis_title="", yaxis_title="Revenue (INR)",
                height=380, margin=dict(t=10, b=40),
            )
            fig.update_traces(textposition="outside")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Waiting for first 5-minute window...")

    with rev_right:
        st.markdown("#### Revenue by City")
        if not city_data.empty:
            city_agg = (
                city_data.groupby("city", as_index=False)
                .agg(revenue=("total_revenue", "sum"), orders=("total_orders", "sum"))
                .sort_values("revenue", ascending=False)
            )
            fig = px.bar(
                city_agg, x="city", y="revenue", color="city",
                text_auto=".2s",
                color_discrete_sequence=px.colors.qualitative.Pastel,
            )
            fig.update_layout(
                showlegend=False, xaxis_title="", yaxis_title="Revenue (INR)",
                height=380, margin=dict(t=10, b=40),
            )
            fig.update_traces(textposition="outside")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Waiting for first 5-minute window...")

    st.markdown("#### Revenue Windows Timeline")
    if not rev_data.empty:
        fig = px.bar(
            rev_data.sort_values("window_start"),
            x="window_start", y="total_revenue", color="category",
            barmode="stack",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig.update_layout(
            height=340, margin=dict(t=10, b=40),
            xaxis_title="Window", yaxis_title="Revenue (INR)",
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
        )
        st.plotly_chart(fig, use_container_width=True)

    with st.expander("Raw Revenue Data"):
        if not rev_data.empty:
            st.dataframe(rev_data, use_container_width=True, hide_index=True)

# ===========================================================================
# TAB 2: LIVE ORDER FEED
# ===========================================================================
with tab_orders:
    st.markdown("#### Latest Enriched Orders")
    if not orders_sample.empty:
        latest = orders_sample.sort_values("event_time", ascending=False).head(50)

        def _pay_style(val):
            m = {
                "FAILED": "background-color:#fee2e2;color:#991b1b",
                "PENDING": "background-color:#fef3c7;color:#92400e",
                "SUCCESS": "background-color:#d1fae5;color:#065f46",
            }
            return m.get(val, "")

        display_cols = [
            "order_id", "customer_name", "product_name", "category", "city",
            "order_amount", "payment_status", "payment_method", "device_type", "event_time"
        ]
        show_df = latest[[c for c in display_cols if c in latest.columns]]
        styled = show_df.style.map(_pay_style, subset=["payment_status"])
        styled = styled.format({"order_amount": "₹{:,.2f}"})
        st.dataframe(styled, use_container_width=True, hide_index=True, height=480)

    ol, or_ = st.columns(2)

    with ol:
        st.markdown("#### Payment Status")
        if not orders_sample.empty:
            pay_counts = orders_sample["payment_status"].value_counts().reset_index()
            pay_counts.columns = ["status", "count"]
            fig = px.pie(
                pay_counts, names="status", values="count",
                color="status",
                color_discrete_map={
                    "SUCCESS": "#10b981", "FAILED": "#ef4444", "PENDING": "#f59e0b"
                },
                hole=0.45,
            )
            fig.update_layout(height=300, margin=dict(t=10, b=10))
            st.plotly_chart(fig, use_container_width=True)

    with or_:
        st.markdown("#### Device Distribution")
        if not orders_sample.empty:
            dev_counts = orders_sample["device_type"].value_counts().reset_index()
            dev_counts.columns = ["device", "count"]
            fig = px.pie(
                dev_counts, names="device", values="count",
                color="device",
                color_discrete_map={
                    "ANDROID": "#3b82f6", "IOS": "#8b5cf6", "WEB": "#06b6d4"
                },
                hole=0.45,
            )
            fig.update_layout(height=300, margin=dict(t=10, b=10))
            st.plotly_chart(fig, use_container_width=True)

# ===========================================================================
# TAB 3: ALERTS
# ===========================================================================
with tab_alerts:
    al, ar = st.columns(2)

    with al:
        st.markdown("#### High-Value Orders (>= ₹12,999)")
        hv = data["hv_orders"]
        if not hv.empty:
            hv = to_numeric_cols(hv, ["order_amount"])
            hv = hv.sort_values("order_amount", ascending=False)
            styled = hv.style.format({"order_amount": "₹{:,.2f}"})
            styled = styled.background_gradient(subset=["order_amount"], cmap="YlOrRd")
            st.dataframe(styled, use_container_width=True, hide_index=True, height=440)
        else:
            st.info("No high-value orders yet...")

    with ar:
        st.markdown("#### Suspicious Orders")
        susp = data["susp_orders"]
        if not susp.empty:
            susp = to_numeric_cols(susp, ["order_amount"])
            susp = susp.sort_values("order_amount", ascending=False)
            styled = susp.style.format({"order_amount": "₹{:,.2f}"})
            st.dataframe(styled, use_container_width=True, hide_index=True, height=440)
        else:
            st.info("No suspicious orders yet...")

    st.markdown("#### Fraud Signal Breakdown")
    if not susp.empty:
        reason_counts = susp["reason"].value_counts().reset_index()
        reason_counts.columns = ["reason", "count"]
        fig = px.bar(
            reason_counts, x="count", y="reason", orientation="h",
            color="reason",
            color_discrete_sequence=px.colors.qualitative.Set1,
        )
        fig.update_layout(
            showlegend=False, height=260, margin=dict(t=10, b=10, l=10),
            xaxis_title="Count", yaxis_title="",
        )
        st.plotly_chart(fig, use_container_width=True)

# ===========================================================================
# TAB 4: DEEP ANALYTICS (uses single orders_sample for all distributions)
# ===========================================================================
with tab_deep:
    dl, dr = st.columns(2)

    with dl:
        st.markdown("#### Revenue by Payment Method")
        if not orders_sample.empty:
            pay_agg = (
                orders_sample.groupby("payment_method", as_index=False)
                .agg(revenue=("order_amount", "sum"), orders=("order_amount", "count"))
            ).sort_values("revenue", ascending=False)
            fig = px.bar(
                pay_agg, x="payment_method", y="revenue",
                text_auto=".2s", color="payment_method",
                color_discrete_sequence=px.colors.qualitative.Bold,
            )
            fig.update_layout(
                showlegend=False, height=340, margin=dict(t=10, b=40),
                xaxis_title="", yaxis_title="Revenue (INR)",
            )
            fig.update_traces(textposition="outside")
            st.plotly_chart(fig, use_container_width=True)

    with dr:
        st.markdown("#### Revenue by Loyalty Tier")
        if not orders_sample.empty and "loyalty_tier" in orders_sample.columns:
            tier_agg = (
                orders_sample.groupby("loyalty_tier", as_index=False)
                .agg(revenue=("order_amount", "sum"), orders=("order_amount", "count"))
            ).sort_values("revenue", ascending=False)
            tier_order = ["PLATINUM", "GOLD", "SILVER", "BRONZE"]
            tier_agg["loyalty_tier"] = pd.Categorical(
                tier_agg["loyalty_tier"], categories=tier_order, ordered=True
            )
            tier_agg = tier_agg.sort_values("loyalty_tier")
            fig = px.bar(
                tier_agg, x="loyalty_tier", y="revenue",
                text_auto=".2s", color="loyalty_tier",
                color_discrete_map={
                    "PLATINUM": "#a78bfa", "GOLD": "#fbbf24",
                    "SILVER": "#94a3b8", "BRONZE": "#d97706",
                },
            )
            fig.update_layout(
                showlegend=False, height=340, margin=dict(t=10, b=40),
                xaxis_title="", yaxis_title="Revenue (INR)",
            )
            fig.update_traces(textposition="outside")
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("#### Top Products by Revenue")
    if not orders_sample.empty:
        prod_agg = (
            orders_sample.groupby(
                ["product_name", "category"], as_index=False, dropna=False
            )
            .agg(revenue=("order_amount", "sum"), orders=("order_amount", "count"))
            .sort_values("revenue", ascending=False)
            .head(15)
        )
        fig = px.bar(
            prod_agg, x="revenue", y="product_name", orientation="h",
            color="category", text_auto=".2s",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig.update_layout(
            height=420, margin=dict(t=10, b=10, l=10),
            xaxis_title="Revenue (INR)", yaxis_title="",
            yaxis=dict(categoryorder="total ascending"),
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
        )
        st.plotly_chart(fig, use_container_width=True)

    d2l, d2r = st.columns(2)

    with d2l:
        st.markdown("#### Customer Profiles")
        customers = data["customers"]
        if not customers.empty:
            st.dataframe(customers, use_container_width=True, hide_index=True, height=300)

    with d2r:
        st.markdown("#### Product Catalog (by Inventory)")
        products = data["products"]
        if not products.empty:
            products = to_numeric_cols(products, ["unit_price", "inventory_count"])
            products = products.sort_values("inventory_count")

            def _inv_style(val):
                if isinstance(val, (int, float)) and val < 100:
                    return "background-color:#fee2e2;color:#991b1b;font-weight:bold"
                return ""

            styled = products.style.map(_inv_style, subset=["inventory_count"])
            styled = styled.format({"unit_price": "₹{:,.2f}"})
            st.dataframe(styled, use_container_width=True, hide_index=True, height=300)

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.markdown("---")
f1, f2, f3 = st.columns(3)
with f1:
    st.caption(f"Last refreshed: {datetime.now().strftime('%H:%M:%S')}")
with f2:
    st.caption("Apache Fluss 0.9.0 + Apache Flink 1.20")
with f3:
    st.caption(f"Gateway: {gateway_url}")

last_refresh_slot.markdown(f"**Last refresh:** {datetime.now().strftime('%H:%M:%S')}")

time.sleep(refresh_interval)
st.rerun()
