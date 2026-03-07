"""
DAYS 11-12: Streamlit Dashboard
Tajir Retail — Kiryana Store Demand Forecasting & Inventory Optimization

LOCAL:  streamlit run dashboard/app.py  (uses full 2.6M rows)
CLOUD:  Streamlit Community Cloud       (uses sampled data)

5 Pages:
  1. Executive Overview
  2. Stockout Analysis
  3. Demand Forecasting
  4. Reorder Alerts
  5. Store Deep Dive
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import os
import warnings
warnings.filterwarnings('ignore')


# ============================================
# PAGE CONFIG
# ============================================
st.set_page_config(
    page_title="Tajir Retail — Demand Forecasting",
    page_icon="🏪",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================
# CUSTOM CSS
# ============================================
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
        font-size: 1.0rem;
        color: #666;
        text-align: center;
        margin-bottom: 1.5rem;
    }
    .stMetric > div {
        background-color: #f8f9fa;
        padding: 12px;
        border-radius: 8px;
        border: 1px solid #e9ecef;
    }
</style>
""", unsafe_allow_html=True)


# ============================================
# SMART DATA LOADING
# Tries full data first (local), falls back to cloud data
# ============================================
@st.cache_data
def load_main_data():
    paths = [
        'data/processed/ml_ready_data.csv',
        'data/cloud/main_data.csv',
        '../data/processed/ml_ready_data.csv',
        '../data/cloud/main_data.csv',
    ]
    for path in paths:
        if os.path.exists(path):
            try:
                df = pd.read_csv(path, parse_dates=['date'])
                return df, path
            except Exception as e:
                st.warning(f"Found {path} but failed to read: {e}")
                # Try without date parsing
                try:
                    df = pd.read_csv(path)
                    if 'date' in df.columns:
                        df['date'] = pd.to_datetime(df['date'], errors='coerce')
                    return df, path
                except Exception as e2:
                    st.error(f"Second attempt failed: {e2}")
                    continue
    return None, None
import glob
st.sidebar.markdown("### DEBUG")
st.sidebar.write("CWD:", os.getcwd())
st.sidebar.write("Files in data/cloud/:", os.listdir("data/cloud") if os.path.exists("data/cloud") else "FOLDER NOT FOUND")
st.sidebar.write("Files in root:", [f for f in os.listdir(".") if not f.startswith(".")])


@st.cache_data
def load_csv(name):
    paths = [
        f'data/processed/{name}',
        f'data/cloud/{name}',
        f'../data/processed/{name}',
        f'../data/cloud/{name}',
    ]
    for path in paths:
        if os.path.exists(path):
            return pd.read_csv(path)
    return None


@st.cache_data
def load_model_metrics():
    # Try CSV first (cloud), then pickle (local)
    for path in ['data/cloud/model_metrics.csv', '../data/cloud/model_metrics.csv']:
        if os.path.exists(path):
            return pd.read_csv(path).iloc[0].to_dict()

    for path in ['models/gradient_boosting_model.pkl', '../models/gradient_boosting_model.pkl',
                  'models/demand_forecaster.pkl', '../models/demand_forecaster.pkl']:
        if os.path.exists(path):
            try:
                import pickle
                with open(path, 'rb') as f:
                    data = pickle.load(f)
                return data.get('metrics', {})
            except Exception:
                pass
    return {}


# Load all data
df, data_source = load_main_data()
predictions = load_csv('predictions.csv')
feature_imp = load_csv('feature_importance.csv')
stockout_risk = load_csv('stockout_risk_assessment.csv')
reorder_recs = load_csv('reorder_recommendations.csv')
alerts = load_csv('active_reorder_alerts.csv')
model_metrics = load_model_metrics()

if df is None:
    st.error("Data not found! Run ETL pipeline first or check data folder.")
    st.stop()


# ============================================
# SIDEBAR
# ============================================
st.sidebar.markdown("## 🏪 Tajir Retail")
st.sidebar.markdown("**Demand Forecasting & Inventory Optimization**")
st.sidebar.markdown("---")

# Show data source
if data_source and 'ml_ready_data' in data_source:
    st.sidebar.success(f"Running on FULL dataset\n{len(df):,} rows")
else:
    st.sidebar.info(f"Running on CLOUD dataset\n{len(df):,} rows")

page = st.sidebar.radio(
    "Navigate",
    ["📊 Executive Overview", "🚨 Stockout Analysis", "🤖 Demand Forecasting",
     "📦 Reorder Alerts", "🏬 Store Deep Dive"]
)

st.sidebar.markdown("---")
st.sidebar.markdown("### Filters")

# Store filter
all_stores = sorted(df['store_id'].unique())
selected_stores = st.sidebar.multiselect(
    "Select Stores", all_stores,
    default=all_stores
)

# Category filter
if 'tajir_category' in df.columns:
    all_categories = sorted(df['tajir_category'].dropna().unique())
    selected_categories = st.sidebar.multiselect(
        "Select Categories", all_categories, default=all_categories
    )
else:
    selected_categories = None

# Year filter
all_years = sorted(df['date'].dt.year.unique())
selected_years = st.sidebar.multiselect(
    "Select Years", all_years, default=all_years
)

# Apply filters
filtered_df = df[
    (df['store_id'].isin(selected_stores)) &
    (df['date'].dt.year.isin(selected_years))
]
if selected_categories and 'tajir_category' in df.columns:
    filtered_df = filtered_df[filtered_df['tajir_category'].isin(selected_categories)]

st.sidebar.markdown("---")
st.sidebar.markdown(f"**Showing:** {len(filtered_df):,} / {len(df):,} records")
st.sidebar.markdown(f"**Date:** {df['date'].min().date()} to {df['date'].max().date()}")


# ============================================
# PAGE 1: EXECUTIVE OVERVIEW
# ============================================
if page == "📊 Executive Overview":
    st.markdown('<div class="main-header">📊 Executive Overview</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Tajir Retail — Kiryana Store Demand Forecasting & Inventory Optimization</div>',
                unsafe_allow_html=True)

    # KPIs
    col1, col2, col3, col4, col5 = st.columns(5)
    total_revenue = filtered_df['sales'].sum()
    avg_daily = filtered_df['sales'].mean()
    n_stores = filtered_df['store_id'].nunique()
    n_products = filtered_df['family'].nunique()
    stockout_rate = filtered_df['is_zero_sale'].mean() * 100 if 'is_zero_sale' in filtered_df.columns else 0

    col1.metric("💰 Total Revenue", f"${total_revenue:,.0f}")
    col2.metric("📈 Avg Daily Sales", f"${avg_daily:,.1f}")
    col3.metric("🏪 Active Stores", f"{n_stores}")
    col4.metric("📦 Product Families", f"{n_products}")
    col5.metric("🚨 Stockout Rate", f"{stockout_rate:.1f}%")

    st.markdown("---")

    # Charts Row 1
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("📈 Monthly Sales Trend")
        monthly = filtered_df.groupby(filtered_df['date'].dt.to_period('M').astype(str))['sales'].sum().reset_index()
        monthly.columns = ['month', 'sales']
        fig = px.line(monthly, x='month', y='sales')
        fig.update_layout(xaxis_tickangle=-45, height=400, xaxis_title='Month', yaxis_title='Total Sales')
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("🏪 Sales by Store Type")
        if 'store_type' in filtered_df.columns:
            by_type = filtered_df.groupby('store_type')['sales'].agg(['mean', 'sum']).reset_index()
            by_type.columns = ['Store Type', 'Avg Sales', 'Total Sales']
            fig = px.bar(by_type, x='Store Type', y='Total Sales',
                        color='Avg Sales', color_continuous_scale='Viridis')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    # Charts Row 2
    col_left2, col_right2 = st.columns(2)

    with col_left2:
        st.subheader("🏆 Top 10 Products by Revenue")
        top_prod = filtered_df.groupby('family')['sales'].sum().sort_values(ascending=True).tail(10).reset_index()
        fig = px.bar(top_prod, x='sales', y='family', orientation='h',
                    color='sales', color_continuous_scale='Blues')
        fig.update_layout(height=400, yaxis_title='', xaxis_title='Total Sales')
        st.plotly_chart(fig, use_container_width=True)

    with col_right2:
        st.subheader("📅 Day of Week Pattern")
        if 'day_name' in filtered_df.columns:
            day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            by_day = filtered_df.groupby('day_name')['sales'].mean().reindex(day_order).reset_index()
            by_day.columns = ['Day', 'Avg Sales']
            fig = px.bar(by_day, x='Day', y='Avg Sales', color='Avg Sales', color_continuous_scale='Sunset')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    # Pakistan Seasons
    st.markdown("---")
    st.subheader("🇵🇰 Pakistan Seasonal Impact on Sales")

    season_cols = {
        'is_ramadan_period': 'Ramadan',
        'is_wedding_season': 'Wedding Season',
        'is_summer_peak': 'Summer Peak',
        'is_payday_week': 'Payday Week',
        'is_school_season': 'School Season'
    }

    season_data = []
    for col, name in season_cols.items():
        if col in filtered_df.columns:
            in_s = filtered_df[filtered_df[col] == 1]['sales'].mean()
            out_s = filtered_df[filtered_df[col] == 0]['sales'].mean()
            impact = ((in_s - out_s) / out_s * 100) if out_s > 0 else 0
            season_data.append({'Season': name, 'During Season': round(in_s, 1),
                              'Normal Period': round(out_s, 1), 'Impact (%)': round(impact, 1)})

    if season_data:
        season_df = pd.DataFrame(season_data)
        col_s1, col_s2 = st.columns(2)

        with col_s1:
            fig = px.bar(season_df, x='Season', y=['During Season', 'Normal Period'],
                        barmode='group', color_discrete_sequence=['#E74C3C', '#3498DB'])
            fig.update_layout(height=400, yaxis_title='Avg Daily Sales')
            st.plotly_chart(fig, use_container_width=True)

        with col_s2:
            colors = ['#E74C3C' if x > 0 else '#2ECC71' for x in season_df['Impact (%)']]
            fig = go.Figure(go.Bar(x=season_df['Impact (%)'], y=season_df['Season'],
                                  orientation='h', marker_color=colors))
            fig.update_layout(title='Sales Impact (%)', height=400, xaxis_title='Impact (%)')
            st.plotly_chart(fig, use_container_width=True)


# ============================================
# PAGE 2: STOCKOUT ANALYSIS
# ============================================
elif page == "🚨 Stockout Analysis":
    st.markdown('<div class="main-header">🚨 Stockout Analysis</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Identifying where revenue is lost due to stockouts</div>',
                unsafe_allow_html=True)

    # KPIs
    col1, col2, col3, col4 = st.columns(4)
    total_days = len(filtered_df)
    stockout_days = int(filtered_df['is_zero_sale'].sum()) if 'is_zero_sale' in filtered_df.columns else 0
    stockout_pct = stockout_days / total_days * 100 if total_days > 0 else 0
    avg_selling = filtered_df[filtered_df['sales'] > 0]['sales'].mean() if (filtered_df['sales'] > 0).any() else 0
    est_lost = avg_selling * stockout_days

    col1.metric("📊 Total Days", f"{total_days:,}")
    col2.metric("🚫 Stockout Days", f"{stockout_days:,}")
    col3.metric("📉 Stockout Rate", f"{stockout_pct:.1f}%")
    col4.metric("💸 Est. Lost Revenue", f"${est_lost:,.0f}")

    st.markdown("---")

    col_left, col_right = st.columns(2)
    df, data_source = load_main_data()

# DEBUG: Show what files exist (remove after fixing)
    import glob
    st.sidebar.markdown("### DEBUG")
    st.sidebar.write("CWD:", os.getcwd())
    st.sidebar.write("Files in data/cloud/:", os.listdir("data/cloud") if os.path.exists("data/cloud") else "FOLDER NOT FOUND")
    st.sidebar.write("Files in root:", [f for f in os.listdir(".") if not f.startswith(".")])
    with col_left:
        st.subheader("🏪 Stockout by Store Type")
        if 'store_type' in filtered_df.columns and 'is_zero_sale' in filtered_df.columns:
            by_type = (filtered_df.groupby('store_type')['is_zero_sale'].mean() * 100).reset_index()
            by_type.columns = ['Store Type', 'Stockout Rate (%)']
            fig = px.bar(by_type, x='Store Type', y='Stockout Rate (%)',
                        color='Stockout Rate (%)', color_continuous_scale='Reds')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("📦 Stockout by Category")
        cat_col = 'tajir_category' if 'tajir_category' in filtered_df.columns else 'family'
        if 'is_zero_sale' in filtered_df.columns:
            by_cat = (filtered_df.groupby(cat_col)['is_zero_sale'].mean() * 100).sort_values(ascending=True).reset_index()
            by_cat.columns = ['Category', 'Stockout Rate (%)']
            fig = px.bar(by_cat, x='Stockout Rate (%)', y='Category', orientation='h',
                        color='Stockout Rate (%)', color_continuous_scale='OrRd')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    # Monthly trend
    st.subheader("📅 Monthly Stockout Trend")
    if 'is_zero_sale' in filtered_df.columns:
        monthly_so = (filtered_df.groupby(filtered_df['date'].dt.to_period('M').astype(str))['is_zero_sale'].mean() * 100).reset_index()
        monthly_so.columns = ['Month', 'Stockout Rate (%)']
        fig = px.line(monthly_so, x='Month', y='Stockout Rate (%)')
        fig.update_layout(xaxis_tickangle=-45, height=400)
        st.plotly_chart(fig, use_container_width=True)

    # Heatmap
    st.subheader("🔥 Stockout Heatmap: Store x Category")
    if 'is_zero_sale' in filtered_df.columns:
        top_stores_so = filtered_df.groupby('store_id')['is_zero_sale'].mean().sort_values(ascending=False).head(15).index
        hm_data = filtered_df[filtered_df['store_id'].isin(top_stores_so)]
        cat_col = 'tajir_category' if 'tajir_category' in hm_data.columns else 'family'
        pivot = (hm_data.groupby(['store_id', cat_col])['is_zero_sale'].mean() * 100).unstack(fill_value=0)

        fig = px.imshow(pivot.values,
                       labels=dict(x="Category", y="Store ID", color="Stockout %"),
                       x=list(pivot.columns), y=[str(s) for s in pivot.index],
                       color_continuous_scale='RdYlGn_r')
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

    # Risk table
    if stockout_risk is not None:
        st.subheader("⚠️ Risk Assessment Table")
        display_cols = [c for c in ['store_id', 'family', 'city', 'stockout_rate',
                                     'risk_score', 'risk_category', 'avg_sales'] if c in stockout_risk.columns]
        st.dataframe(stockout_risk[display_cols].head(50), use_container_width=True, height=400)


# ============================================
# PAGE 3: DEMAND FORECASTING
# ============================================
elif page == "🤖 Demand Forecasting":
    st.markdown('<div class="main-header">🤖 Demand Forecasting Results</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">XGBoost model trained on 2.6M+ records</div>',
                unsafe_allow_html=True)

    # Model metrics
    col1, col2, col3, col4 = st.columns(4)
    if model_metrics:
        col1.metric("MAE", f"{model_metrics.get('MAE', 0):,.2f}")
        col2.metric("RMSE", f"{model_metrics.get('RMSE', 0):,.2f}")
        col3.metric("R² Score", f"{model_metrics.get('R2', 0):.4f}")
        col4.metric("MAPE", f"{model_metrics.get('MAPE', 0):.2f}%")
    else:
        col1.metric("MAE", "N/A")
        col2.metric("RMSE", "N/A")
        col3.metric("R² Score", "N/A")
        col4.metric("MAPE", "N/A")

    st.markdown("---")

    if predictions is not None:
        col_left, col_right = st.columns(2)

        with col_left:
            st.subheader("📈 Actual vs Predicted")
            sample = predictions.sample(min(5000, len(predictions)), random_state=42)
            fig = px.scatter(sample, x='actual', y='predicted', opacity=0.2)
            max_val = max(sample['actual'].max(), sample['predicted'].max())
            fig.add_shape(type='line', x0=0, y0=0, x1=max_val, y1=max_val,
                         line=dict(color='red', dash='dash', width=2))
            fig.update_layout(height=450, xaxis_title='Actual Sales', yaxis_title='Predicted Sales')
            st.plotly_chart(fig, use_container_width=True)

        with col_right:
            st.subheader("📊 Error Distribution")
            errors = predictions['actual'] - predictions['predicted']
            fig = px.histogram(errors, nbins=100, color_discrete_sequence=['#27AE60'])
            fig.add_vline(x=0, line_dash='dash', line_color='red', line_width=2)
            fig.update_layout(height=450, xaxis_title='Error', yaxis_title='Frequency', showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        # Timeline
        st.subheader("📅 Predictions Over Time")
        n_groups = 100
        group_size = max(1, len(predictions) // n_groups)
        timeline = pd.DataFrame({
            'Period': range(n_groups),
            'Actual': [predictions['actual'].iloc[i*group_size:(i+1)*group_size].mean() for i in range(n_groups)],
            'Predicted': [predictions['predicted'].iloc[i*group_size:(i+1)*group_size].mean() for i in range(n_groups)]
        })
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=timeline['Period'], y=timeline['Actual'],
                                name='Actual', line=dict(color='#2E86C1', width=2)))
        fig.add_trace(go.Scatter(x=timeline['Period'], y=timeline['Predicted'],
                                name='Predicted', line=dict(color='#E74C3C', width=2)))
        fig.update_layout(height=400, xaxis_title='Time Period', yaxis_title='Average Sales')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No predictions found. Run demand_forecasting.py first.")

    # Feature Importance
    if feature_imp is not None:
        st.subheader("🏆 Feature Importance")
        top_n = st.slider("Show top N features", 10, min(30, len(feature_imp)), 20)
        top_f = feature_imp.head(top_n).sort_values('importance', ascending=True)

        pakistan_kw = ['ramadan', 'eid', 'wedding', 'payday', 'friday', 'summer', 'school']
        colors = ['#E74C3C' if any(k in f.lower() for k in pakistan_kw) else '#3498DB' for f in top_f['feature']]

        fig = go.Figure(go.Bar(x=top_f['importance'], y=top_f['feature'], orientation='h', marker_color=colors))
        fig.update_layout(title=f'Top {top_n} Features (Red = Pakistan-specific)',
                         height=max(400, top_n * 25), xaxis_title='Importance')
        st.plotly_chart(fig, use_container_width=True)


# ============================================
# PAGE 4: REORDER ALERTS
# ============================================
elif page == "📦 Reorder Alerts":
    st.markdown('<div class="main-header">📦 Reorder Alert System</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Smart inventory recommendations</div>', unsafe_allow_html=True)

    if alerts is not None and len(alerts) > 0:
        col1, col2, col3, col4 = st.columns(4)
        critical = len(alerts[alerts['alert_level'] == 'CRITICAL'])
        high = len(alerts[alerts['alert_level'] == 'HIGH'])
        medium = len(alerts[alerts['alert_level'] == 'MEDIUM'])
        low = len(alerts[alerts['alert_level'] == 'LOW'])

        col1.metric("🔴 Critical", critical)
        col2.metric("🟠 High", high)
        col3.metric("🟡 Medium", medium)
        col4.metric("🟢 Low", low)

        st.markdown("---")

        severity_filter = st.multiselect("Filter by Level",
            ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW'],
            default=['CRITICAL', 'HIGH', 'MEDIUM', 'LOW'])

        filtered_alerts = alerts[alerts['alert_level'].isin(severity_filter)]

        col_left, col_right = st.columns(2)

        with col_left:
            st.subheader("📊 Alerts by Severity")
            ac = filtered_alerts['alert_level'].value_counts()
            colors_map = {'CRITICAL': '#EF4444', 'HIGH': '#F97316', 'MEDIUM': '#EAB308', 'LOW': '#22C55E'}
            fig = px.bar(x=ac.index, y=ac.values, color=ac.index, color_discrete_map=colors_map)
            fig.update_layout(height=350, showlegend=False, xaxis_title='Level', yaxis_title='Count')
            st.plotly_chart(fig, use_container_width=True)

        with col_right:
            st.subheader("📦 Alerts by Product")
            bp = filtered_alerts['family'].value_counts().head(10)
            fig = px.pie(values=bp.values, names=bp.index)
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("📋 Active Alerts")
        display_cols = [c for c in ['alert_level', 'store_id', 'city', 'family',
                                     'avg_daily_demand', 'suggested_order',
                                     'consecutive_zeros', 'alert_reason'] if c in filtered_alerts.columns]
        st.dataframe(filtered_alerts[display_cols], use_container_width=True, height=500)

        csv_data = filtered_alerts.to_csv(index=False)
        st.download_button("📥 Download Alerts CSV", csv_data, "reorder_alerts.csv", "text/csv")
    else:
        st.info("No alerts. Run stockout_analysis.py to generate.")

    # Reorder table
    if reorder_recs is not None:
        st.markdown("---")
        st.subheader("📊 Reorder Recommendations")
        display_cols = [c for c in ['store_id', 'family', 'city', 'avg_daily_demand',
                                     'reorder_point_95', 'safety_stock_95',
                                     'suggested_order_qty', 'risk_category'] if c in reorder_recs.columns]
        active = reorder_recs[reorder_recs['avg_daily_demand'] > 0] if 'avg_daily_demand' in reorder_recs.columns else reorder_recs
        st.dataframe(active[display_cols].head(50), use_container_width=True, height=400)


# ============================================
# PAGE 5: STORE DEEP DIVE
# ============================================
elif page == "🏬 Store Deep Dive":
    st.markdown('<div class="main-header">🏬 Store Deep Dive</div>', unsafe_allow_html=True)

    selected_store = st.selectbox("Select Store", sorted(df['store_id'].unique()))
    store_data = df[df['store_id'] == selected_store]

    if len(store_data) > 0:
        col1, col2, col3, col4, col5 = st.columns(5)
        city = store_data['city'].iloc[0] if 'city' in store_data.columns else 'N/A'
        stype = store_data['store_type'].iloc[0] if 'store_type' in store_data.columns else 'N/A'
        rev = store_data['sales'].sum()
        avg = store_data['sales'].mean()
        so_rate = store_data['is_zero_sale'].mean() * 100 if 'is_zero_sale' in store_data.columns else 0

        col1.metric("📍 City", city)
        col2.metric("🏷️ Type", stype)
        col3.metric("💰 Revenue", f"${rev:,.0f}")
        col4.metric("📈 Avg Sales", f"${avg:,.1f}")
        col5.metric("🚨 Stockout", f"{so_rate:.1f}%")

        st.markdown("---")

        col_left, col_right = st.columns(2)

        with col_left:
            st.subheader("📈 Sales Trend")
            monthly = store_data.groupby(store_data['date'].dt.to_period('M').astype(str))['sales'].sum().reset_index()
            monthly.columns = ['Month', 'Sales']
            fig = px.line(monthly, x='Month', y='Sales')
            fig.update_layout(xaxis_tickangle=-45, height=400)
            st.plotly_chart(fig, use_container_width=True)

        with col_right:
            st.subheader("📦 Top Products")
            bp = store_data.groupby('family')['sales'].sum().sort_values(ascending=True).tail(10).reset_index()
            bp.columns = ['Product', 'Sales']
            fig = px.bar(bp, x='Sales', y='Product', orientation='h',
                        color='Sales', color_continuous_scale='Blues')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        # Stockout by product
        st.subheader(f"🚨 Stockout by Product — Store {selected_store}")
        if 'is_zero_sale' in store_data.columns:
            by_fam = store_data.groupby('family').agg(
                total=('sales', 'count'), stockouts=('is_zero_sale', 'sum')
            ).reset_index()
            by_fam['rate'] = (by_fam['stockouts'] / by_fam['total'] * 100).round(1)
            by_fam = by_fam.sort_values('rate', ascending=False)
            fig = px.bar(by_fam, x='family', y='rate', color='rate', color_continuous_scale='RdYlGn_r')
            fig.update_layout(xaxis_tickangle=-45, height=400, xaxis_title='Product', yaxis_title='Stockout %')
            st.plotly_chart(fig, use_container_width=True)

        # Store alerts
        if alerts is not None and len(alerts) > 0:
            sa = alerts[alerts['store_id'] == selected_store]
            if len(sa) > 0:
                st.subheader(f"🚨 Active Alerts — Store {selected_store}")
                st.dataframe(sa, use_container_width=True)
            else:
                st.success(f"No active alerts for Store {selected_store}")


# ============================================
# FOOTER
# ============================================
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #888; font-size: 0.85rem;">
    <strong>Tajir Retail — Kiryana Store Demand Forecasting & Inventory Optimization</strong><br>
    Built with Python | SQL Server | XGBoost | Streamlit<br>
    Data: 2.6M+ records | 54 stores | 33 product families | 2013-2017
</div>
""", unsafe_allow_html=True)