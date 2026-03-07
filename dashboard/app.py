"""
Tajir Retail — Kiryana Store Demand Forecasting & Inventory Optimization
Streamlit Dashboard
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import os
import warnings
warnings.filterwarnings('ignore')

# ---------------------------------------------------
# PAGE CONFIG
# ---------------------------------------------------
st.set_page_config(
    page_title="Tajir Retail — Demand Forecasting",
    page_icon="🏪",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ---------------------------------------------------
# STYLE
# ---------------------------------------------------
st.markdown("""
<style>
.main-header {
    font-size: 2.2rem;
    font-weight: 700;
    color: #1a1a2e;
    text-align: center;
    padding: 0.5rem 0;
}
.sub-header {
    font-size: 1rem;
    color: #666;
    text-align: center;
    margin-bottom: 1.5rem;
}
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------
# DATA LOADING
# ---------------------------------------------------

@st.cache_data
def load_main_data():

    paths = [
        'data/processed/ml_ready_data.csv',
        'data/cloud/main_data.csv'
    ]

    for path in paths:
        if os.path.exists(path):

            try:
                df = pd.read_csv(path)

                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'], errors='coerce')

                return df, path

            except:
                continue

    return None, None


@st.cache_data
def load_csv(name):

    for folder in ['data/processed', 'data/cloud']:

        path = os.path.join(folder, name)

        if os.path.exists(path):
            try:
                return pd.read_csv(path)
            except:
                continue

    return None


@st.cache_data
def load_model_metrics():

    path = "data/cloud/model_metrics.csv"

    if os.path.exists(path):

        try:
            return pd.read_csv(path).iloc[0].to_dict()
        except:
            pass

    return {}

# ---------------------------------------------------
# LOAD DATA
# ---------------------------------------------------

df, data_source = load_main_data()

predictions = load_csv("predictions.csv")
feature_imp = load_csv("feature_importance.csv")
stockout_risk = load_csv("stockout_risk_assessment.csv")
reorder_recs = load_csv("reorder_recommendations.csv")
alerts = load_csv("active_reorder_alerts.csv")
model_metrics = load_model_metrics()

if df is None:
    st.error("Dataset not found.")
    st.stop()

# ---------------------------------------------------
# SIDEBAR
# ---------------------------------------------------

st.sidebar.title("🏪 Tajir Retail")
st.sidebar.markdown("Demand Forecasting Dashboard")

st.sidebar.markdown("---")

if data_source and "ml_ready_data" in data_source:
    st.sidebar.success(f"Full Dataset Loaded ({len(df):,} rows)")
else:
    st.sidebar.info(f"Cloud Dataset ({len(df):,} rows)")

page = st.sidebar.radio(
    "Navigation",
    [
        "📊 Executive Overview",
        "🚨 Stockout Analysis",
        "🤖 Demand Forecasting",
        "📦 Reorder Alerts",
        "🏬 Store Deep Dive"
    ]
)

# ---------------------------------------------------
# FILTERS
# ---------------------------------------------------

st.sidebar.markdown("### Filters")

all_stores = sorted(df['store_id'].unique())
selected_stores = st.sidebar.multiselect(
    "Stores",
    all_stores,
    default=all_stores
)

# Category filter
selected_categories = None
if "tajir_category" in df.columns:
    cats = sorted(df['tajir_category'].dropna().unique())

    selected_categories = st.sidebar.multiselect(
        "Categories",
        cats,
        default=cats
    )

# Year filter
years = sorted(df['date'].dropna().dt.year.unique())

selected_years = st.sidebar.multiselect(
    "Years",
    years,
    default=years
)

# ---------------------------------------------------
# APPLY FILTERS
# ---------------------------------------------------

filtered_df = df[
    (df['store_id'].isin(selected_stores)) &
    (df['date'].dt.year.isin(selected_years))
]

if selected_categories is not None:
    filtered_df = filtered_df[
        filtered_df['tajir_category'].isin(selected_categories)
    ]

# ---------------------------------------------------
# EXECUTIVE OVERVIEW
# ---------------------------------------------------

if page == "📊 Executive Overview":

    st.markdown('<div class="main-header">Executive Overview</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Retail Demand Insights</div>', unsafe_allow_html=True)

    total_revenue = filtered_df['sales'].sum()
    avg_daily = filtered_df['sales'].mean()
    n_stores = filtered_df['store_id'].nunique()
    n_products = filtered_df['family'].nunique()

    stockout_rate = 0
    if 'is_zero_sale' in filtered_df.columns:
        stockout_rate = filtered_df['is_zero_sale'].mean()*100

    c1,c2,c3,c4,c5 = st.columns(5)

    c1.metric("Total Revenue", f"${total_revenue:,.0f}")
    c2.metric("Avg Daily Sales", f"${avg_daily:,.1f}")
    c3.metric("Stores", n_stores)
    c4.metric("Products", n_products)
    c5.metric("Stockout Rate", f"{stockout_rate:.1f}%")

    st.markdown("---")

    # Monthly Sales

    monthly = (
        filtered_df
        .groupby(filtered_df['date'].dt.to_period("M"))
        ['sales']
        .sum()
        .reset_index()
    )

    monthly['date'] = monthly['date'].astype(str)

    fig = px.line(monthly,x="date",y="sales")

    fig.update_layout(height=400)

    st.plotly_chart(fig,use_container_width=True)

# ---------------------------------------------------
# STOCKOUT ANALYSIS
# ---------------------------------------------------

elif page == "🚨 Stockout Analysis":

    st.markdown('<div class="main-header">Stockout Analysis</div>', unsafe_allow_html=True)

    if 'is_zero_sale' not in filtered_df.columns:
        st.warning("Stockout column not found")
    else:

        stockout_days = filtered_df['is_zero_sale'].sum()
        total_days = len(filtered_df)

        pct = stockout_days/total_days*100

        st.metric("Stockout Rate",f"{pct:.2f}%")

        monthly = (
            filtered_df
            .groupby(filtered_df['date'].dt.to_period("M"))
            ['is_zero_sale']
            .mean()*100
        ).reset_index()

        monthly['date'] = monthly['date'].astype(str)

        fig = px.line(monthly,x="date",y="is_zero_sale")

        st.plotly_chart(fig,use_container_width=True)

# ---------------------------------------------------
# DEMAND FORECASTING
# ---------------------------------------------------

elif page == "🤖 Demand Forecasting":

    st.markdown('<div class="main-header">Demand Forecasting</div>', unsafe_allow_html=True)

    c1,c2,c3,c4 = st.columns(4)

    c1.metric("MAE",model_metrics.get("MAE","N/A"))
    c2.metric("RMSE",model_metrics.get("RMSE","N/A"))
    c3.metric("R²",model_metrics.get("R2","N/A"))
    c4.metric("MAPE",model_metrics.get("MAPE","N/A"))

    if predictions is not None:

        st.subheader("Actual vs Predicted")

        sample = predictions.sample(
            min(5000,len(predictions)),
            random_state=42
        )

        fig = px.scatter(
            sample,
            x="actual",
            y="predicted",
            opacity=0.2
        )

        st.plotly_chart(fig,use_container_width=True)

# ---------------------------------------------------
# REORDER ALERTS
# ---------------------------------------------------

elif page == "📦 Reorder Alerts":

    st.markdown('<div class="main-header">Reorder Alert System</div>', unsafe_allow_html=True)

    if alerts is None:
        st.info("No alerts available")
    else:

        counts = alerts['alert_level'].value_counts()

        fig = px.bar(
            x=counts.index,
            y=counts.values,
            color=counts.index
        )

        st.plotly_chart(fig,use_container_width=True)

        st.dataframe(alerts,use_container_width=True)

# ---------------------------------------------------
# STORE DEEP DIVE
# ---------------------------------------------------

elif page == "🏬 Store Deep Dive":

    st.markdown('<div class="main-header">Store Deep Dive</div>', unsafe_allow_html=True)

    store = st.selectbox(
        "Select Store",
        sorted(df['store_id'].unique())
    )

    store_data = df[df['store_id']==store]

    revenue = store_data['sales'].sum()
    avg = store_data['sales'].mean()

    stockout = 0
    if 'is_zero_sale' in store_data.columns:
        stockout = store_data['is_zero_sale'].mean()*100

    c1,c2,c3 = st.columns(3)

    c1.metric("Revenue",f"${revenue:,.0f}")
    c2.metric("Avg Sales",f"${avg:,.2f}")
    c3.metric("Stockout Rate",f"{stockout:.2f}%")

    monthly = (
        store_data
        .groupby(store_data['date'].dt.to_period("M"))
        ['sales']
        .sum()
        .reset_index()
    )

    monthly['date'] = monthly['date'].astype(str)

    fig = px.line(monthly,x="date",y="sales")

    st.plotly_chart(fig,use_container_width=True)

# ---------------------------------------------------
# FOOTER
# ---------------------------------------------------

st.markdown("---")

st.markdown(
"""
<center>
Tajir Retail — Demand Forecasting & Inventory Optimization  
Built with Python • SQL • Machine Learning • Streamlit
</center>
""",
unsafe_allow_html=True
)