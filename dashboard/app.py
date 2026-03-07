"""
DAYS 11-12: Streamlit Dashboard
Tajir Retail — Kiryana Store Demand Forecasting & Inventory Optimization

Run locally:  streamlit run dashboard/app.py
Deploy:       Streamlit Community Cloud (FREE)

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
from plotly.subplots import make_subplots
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
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.2rem;
        border-radius: 12px;
        color: white;
        text-align: center;
    }
    .alert-critical { background-color: #fee2e2; border-left: 4px solid #ef4444; padding: 10px; margin: 5px 0; border-radius: 4px; }
    .alert-high { background-color: #ffedd5; border-left: 4px solid #f97316; padding: 10px; margin: 5px 0; border-radius: 4px; }
    .alert-medium { background-color: #fef9c3; border-left: 4px solid #eab308; padding: 10px; margin: 5px 0; border-radius: 4px; }
    .alert-low { background-color: #dcfce7; border-left: 4px solid #22c55e; padding: 10px; margin: 5px 0; border-radius: 4px; }
    .stMetric > div { background-color: #f8f9fa; padding: 12px; border-radius: 8px; border: 1px solid #e9ecef; }
</style>
""", unsafe_allow_html=True)


# ============================================
# DATA LOADING (with caching)
# ============================================
@st.cache_data
def load_main_data():
    """Load the main dataset."""
    paths = [
        'data/processed/ml_ready_data.csv',
        'data/cleaned/featured_retail_data.csv',
        '../data/processed/ml_ready_data.csv',
        '../data/cleaned/featured_retail_data.csv'
    ]
    for path in paths:
        if os.path.exists(path):
            df = pd.read_csv(path, parse_dates=['date'])
            return df
    return None


@st.cache_data
def load_predictions():
    """Load model predictions."""
    paths = ['data/processed/predictions.csv', '../data/processed/predictions.csv']
    for path in paths:
        if os.path.exists(path):
            return pd.read_csv(path)
    return None


@st.cache_data
def load_feature_importance():
    """Load feature importance."""
    paths = ['data/processed/feature_importance.csv', '../data/processed/feature_importance.csv']
    for path in paths:
        if os.path.exists(path):
            return pd.read_csv(path)
    return None


@st.cache_data
def load_stockout_risk():
    """Load stockout risk assessment."""
    paths = ['data/processed/stockout_risk_assessment.csv', '../data/processed/stockout_risk_assessment.csv']
    for path in paths:
        if os.path.exists(path):
            return pd.read_csv(path)
    return None


@st.cache_data
def load_reorder_recommendations():
    """Load reorder recommendations."""
    paths = ['data/processed/reorder_recommendations.csv', '../data/processed/reorder_recommendations.csv']
    for path in paths:
        if os.path.exists(path):
            return pd.read_csv(path)
    return None


@st.cache_data
def load_alerts():
    """Load active alerts."""
    paths = ['data/processed/active_reorder_alerts.csv', '../data/processed/active_reorder_alerts.csv']
    for path in paths:
        if os.path.exists(path):
            return pd.read_csv(path)
    return None


# Load all data
df = load_main_data()
predictions = load_predictions()
feature_imp = load_feature_importance()
stockout_risk = load_stockout_risk()
reorder_recs = load_reorder_recommendations()
alerts = load_alerts()

if df is None:
    st.error("Data not found! Please run the ETL pipeline first: `python src/etl_pipeline.py`")
    st.stop()


# ============================================
# SIDEBAR
# ============================================
st.sidebar.markdown("## 🏪 Tajir Retail")
st.sidebar.markdown("**Demand Forecasting & Inventory Optimization**")
st.sidebar.markdown("---")

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
    default=all_stores[:5] if len(all_stores) > 5 else all_stores
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
st.sidebar.markdown(f"**Stores:** {len(selected_stores)} selected")
st.sidebar.markdown(f"**Date:** {df['date'].min().date()} to {df['date'].max().date()}")


# ============================================
# PAGE 1: EXECUTIVE OVERVIEW
# ============================================
if page == "📊 Executive Overview":
    st.markdown('<div class="main-header">📊 Executive Overview</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Tajir Retail — Kiryana Store Demand Forecasting & Inventory Optimization</div>',
                unsafe_allow_html=True)

    # KPI Row
    col1, col2, col3, col4, col5 = st.columns(5)

    total_revenue = filtered_df['sales'].sum()
    avg_daily_sales = filtered_df['sales'].mean()
    total_stores = filtered_df['store_id'].nunique()
    total_products = filtered_df['family'].nunique()
    stockout_rate = filtered_df['is_zero_sale'].mean() * 100 if 'is_zero_sale' in filtered_df.columns else 0

    col1.metric("💰 Total Revenue", f"${total_revenue:,.0f}")
    col2.metric("📈 Avg Daily Sales", f"${avg_daily_sales:,.1f}")
    col3.metric("🏪 Active Stores", f"{total_stores}")
    col4.metric("📦 Product Families", f"{total_products}")
    col5.metric("🚨 Stockout Rate", f"{stockout_rate:.1f}%")

    st.markdown("---")

    # Row 2: Charts
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("📈 Monthly Sales Trend")
        monthly = filtered_df.groupby(filtered_df['date'].dt.to_period('M').astype(str))['sales'].sum().reset_index()
        monthly.columns = ['month', 'sales']
        fig = px.line(monthly, x='month', y='sales', title='Total Sales by Month')
        fig.update_layout(xaxis_title='Month', yaxis_title='Total Sales',
                         xaxis_tickangle=-45, height=400)
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("🏪 Sales by Store Type")
        if 'store_type' in filtered_df.columns:
            by_type = filtered_df.groupby('store_type')['sales'].agg(['mean', 'sum']).reset_index()
            by_type.columns = ['Store Type', 'Avg Sales', 'Total Sales']
            fig = px.bar(by_type, x='Store Type', y='Total Sales',
                        color='Avg Sales', title='Revenue by Store Type',
                        color_continuous_scale='Viridis')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    # Row 3
    col_left2, col_right2 = st.columns(2)

    with col_left2:
        st.subheader("🏆 Top 10 Products by Revenue")
        top_products = filtered_df.groupby('family')['sales'].sum().sort_values(ascending=True).tail(10).reset_index()
        fig = px.bar(top_products, x='sales', y='family', orientation='h',
                    title='Top 10 Product Families', color='sales',
                    color_continuous_scale='Blues')
        fig.update_layout(height=400, yaxis_title='', xaxis_title='Total Sales')
        st.plotly_chart(fig, use_container_width=True)

    with col_right2:
        st.subheader("📅 Day of Week Pattern")
        if 'day_name' in filtered_df.columns:
            day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            by_day = filtered_df.groupby('day_name')['sales'].mean().reindex(day_order).reset_index()
            by_day.columns = ['Day', 'Avg Sales']
            fig = px.bar(by_day, x='Day', y='Avg Sales', title='Average Sales by Day',
                        color='Avg Sales', color_continuous_scale='Sunset')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    # Row 4: Pakistan Insights
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
            in_season = filtered_df[filtered_df[col] == 1]['sales'].mean()
            out_season = filtered_df[filtered_df[col] == 0]['sales'].mean()
            impact = ((in_season - out_season) / out_season * 100) if out_season > 0 else 0
            season_data.append({
                'Season': name,
                'During Season': round(in_season, 1),
                'Normal Period': round(out_season, 1),
                'Impact (%)': round(impact, 1)
            })

    if season_data:
        season_df = pd.DataFrame(season_data)
        col_s1, col_s2 = st.columns(2)

        with col_s1:
            fig = px.bar(season_df, x='Season', y=['During Season', 'Normal Period'],
                        barmode='group', title='Average Sales: Season vs Normal',
                        color_discrete_sequence=['#E74C3C', '#3498DB'])
            fig.update_layout(height=400, yaxis_title='Average Daily Sales')
            st.plotly_chart(fig, use_container_width=True)

        with col_s2:
            colors = ['#E74C3C' if x > 0 else '#2ECC71' for x in season_df['Impact (%)']]
            fig = go.Figure(go.Bar(
                x=season_df['Impact (%)'], y=season_df['Season'],
                orientation='h', marker_color=colors
            ))
            fig.update_layout(title='Sales Impact by Pakistan Season (%)',
                            height=400, xaxis_title='Impact (%)')
            st.plotly_chart(fig, use_container_width=True)


# ============================================
# PAGE 2: STOCKOUT ANALYSIS
# ============================================
elif page == "🚨 Stockout Analysis":
    st.markdown('<div class="main-header">🚨 Stockout Analysis</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Identifying where revenue is being lost due to stockouts</div>',
                unsafe_allow_html=True)

    # KPIs
    col1, col2, col3, col4 = st.columns(4)

    total_days = len(filtered_df)
    stockout_days = filtered_df['is_zero_sale'].sum() if 'is_zero_sale' in filtered_df.columns else 0
    stockout_pct = stockout_days / total_days * 100 if total_days > 0 else 0

    # Estimated lost revenue
    avg_when_selling = filtered_df[filtered_df['sales'] > 0]['sales'].mean() if (filtered_df['sales'] > 0).any() else 0
    est_lost = avg_when_selling * stockout_days

    col1.metric("📊 Total Store-Product Days", f"{total_days:,}")
    col2.metric("🚫 Stockout Days", f"{int(stockout_days):,}")
    col3.metric("📉 Stockout Rate", f"{stockout_pct:.1f}%")
    col4.metric("💸 Est. Lost Revenue", f"${est_lost:,.0f}")

    st.markdown("---")

    # Charts
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("🏪 Stockout Rate by Store Type")
        if 'store_type' in filtered_df.columns and 'is_zero_sale' in filtered_df.columns:
            by_type = filtered_df.groupby('store_type')['is_zero_sale'].mean() * 100
            by_type = by_type.reset_index()
            by_type.columns = ['Store Type', 'Stockout Rate (%)']
            fig = px.bar(by_type, x='Store Type', y='Stockout Rate (%)',
                        color='Stockout Rate (%)', color_continuous_scale='Reds',
                        title='Smaller stores have higher stockout rates')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("📦 Stockout Rate by Category")
        cat_col = 'tajir_category' if 'tajir_category' in filtered_df.columns else 'family'
        if 'is_zero_sale' in filtered_df.columns:
            by_cat = filtered_df.groupby(cat_col)['is_zero_sale'].mean() * 100
            by_cat = by_cat.sort_values(ascending=True).reset_index()
            by_cat.columns = ['Category', 'Stockout Rate (%)']
            fig = px.bar(by_cat, x='Stockout Rate (%)', y='Category', orientation='h',
                        color='Stockout Rate (%)', color_continuous_scale='OrRd',
                        title='Non-Food items have highest stockout rates')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    # Monthly stockout trend
    st.subheader("📅 Monthly Stockout Trend")
    if 'is_zero_sale' in filtered_df.columns:
        monthly_stockout = filtered_df.groupby(
            filtered_df['date'].dt.to_period('M').astype(str)
        )['is_zero_sale'].mean() * 100
        monthly_stockout = monthly_stockout.reset_index()
        monthly_stockout.columns = ['Month', 'Stockout Rate (%)']
        fig = px.line(monthly_stockout, x='Month', y='Stockout Rate (%)',
                     title='Stockout Rate Over Time')
        fig.update_layout(xaxis_tickangle=-45, height=400)
        st.plotly_chart(fig, use_container_width=True)

    # Heatmap: Store vs Product
    st.subheader("🔥 Stockout Heatmap: Store x Product")
    if 'is_zero_sale' in filtered_df.columns:
        # Top 15 stores by stockout
        top_stores_stockout = filtered_df.groupby('store_id')['is_zero_sale'].mean().sort_values(ascending=False).head(15).index
        heatmap_data = filtered_df[filtered_df['store_id'].isin(top_stores_stockout)]

        cat_col = 'tajir_category' if 'tajir_category' in heatmap_data.columns else 'family'
        pivot = heatmap_data.groupby(['store_id', cat_col])['is_zero_sale'].mean() * 100
        pivot = pivot.unstack(fill_value=0)

        fig = px.imshow(pivot.values,
                       labels=dict(x="Product Category", y="Store ID", color="Stockout %"),
                       x=list(pivot.columns), y=[str(s) for s in pivot.index],
                       color_continuous_scale='RdYlGn_r',
                       title='Top 15 At-Risk Stores: Stockout % by Category')
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

    # Risk Assessment Table
    if stockout_risk is not None:
        st.subheader("⚠️ Risk Assessment Summary")
        risk_display = stockout_risk.head(30)
        display_cols = [c for c in ['store_id', 'family', 'city', 'stockout_rate',
                                     'risk_score', 'risk_category', 'avg_sales'] if c in risk_display.columns]
        st.dataframe(risk_display[display_cols], use_container_width=True, height=400)


# ============================================
# PAGE 3: DEMAND FORECASTING
# ============================================
elif page == "🤖 Demand Forecasting":
    st.markdown('<div class="main-header">🤖 Demand Forecasting Results</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">XGBoost / Gradient Boosting model trained on 2.6M records</div>',
                unsafe_allow_html=True)

    # Model metrics
    col1, col2, col3, col4 = st.columns(4)

    # Try to load model metrics
    model_metrics = None
    model_paths = ['models/gradient_boosting_model.pkl', '../models/gradient_boosting_model.pkl',
                   'models/demand_forecaster.pkl', '../models/demand_forecaster.pkl']
    for path in model_paths:
        if os.path.exists(path):
            try:
                import pickle
                with open(path, 'rb') as f:
                    model_data = pickle.load(f)
                model_metrics = model_data.get('metrics', {})
                break
            except Exception:
                pass

    if model_metrics:
        col1.metric("MAE", f"{model_metrics.get('MAE', 0):,.2f}")
        col2.metric("RMSE", f"{model_metrics.get('RMSE', 0):,.2f}")
        col3.metric("R² Score", f"{model_metrics.get('R2', 0):.4f}")
        col4.metric("MAPE", f"{model_metrics.get('MAPE', 0):.2f}%")
    else:
        col1.metric("MAE", "Run model first")
        col2.metric("RMSE", "—")
        col3.metric("R² Score", "—")
        col4.metric("MAPE", "—")

    st.markdown("---")

    # Actual vs Predicted
    if predictions is not None:
        col_left, col_right = st.columns(2)

        with col_left:
            st.subheader("📈 Actual vs Predicted Sales")
            sample_size = min(5000, len(predictions))
            sample = predictions.sample(sample_size, random_state=42)

            fig = px.scatter(sample, x='actual', y='predicted',
                           opacity=0.2, title='Actual vs Predicted (sample)',
                           labels={'actual': 'Actual Sales', 'predicted': 'Predicted Sales'})
            max_val = max(sample['actual'].max(), sample['predicted'].max())
            fig.add_shape(type='line', x0=0, y0=0, x1=max_val, y1=max_val,
                         line=dict(color='red', dash='dash', width=2))
            fig.update_layout(height=450)
            st.plotly_chart(fig, use_container_width=True)

        with col_right:
            st.subheader("📊 Prediction Error Distribution")
            errors = predictions['actual'] - predictions['predicted']
            fig = px.histogram(errors, nbins=100, title='Error Distribution',
                             labels={'value': 'Error (Actual - Predicted)', 'count': 'Frequency'},
                             color_discrete_sequence=['#27AE60'])
            fig.add_vline(x=0, line_dash='dash', line_color='red', line_width=2)
            fig.update_layout(height=450, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        # Time series comparison
        st.subheader("📅 Predictions Over Time")
        n_groups = 100
        group_size = len(predictions) // n_groups
        timeline = pd.DataFrame({
            'period': range(n_groups),
            'Actual': [predictions['actual'].iloc[i*group_size:(i+1)*group_size].mean() for i in range(n_groups)],
            'Predicted': [predictions['predicted'].iloc[i*group_size:(i+1)*group_size].mean() for i in range(n_groups)]
        })

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=timeline['period'], y=timeline['Actual'],
                                name='Actual', line=dict(color='#2E86C1', width=2)))
        fig.add_trace(go.Scatter(x=timeline['period'], y=timeline['Predicted'],
                                name='Predicted', line=dict(color='#E74C3C', width=2)))
        fig.update_layout(title='Actual vs Predicted (Aggregated Time Periods)',
                         xaxis_title='Time Period', yaxis_title='Average Sales', height=400)
        st.plotly_chart(fig, use_container_width=True)

    # Feature Importance
    if feature_imp is not None:
        st.subheader("🏆 Feature Importance — What Drives Demand?")

        top_n = st.slider("Show top N features", 10, 30, 20)
        top_features = feature_imp.head(top_n).sort_values('importance', ascending=True)

        # Color Pakistan features differently
        pakistan_keywords = ['ramadan', 'eid', 'wedding', 'payday', 'friday', 'summer', 'school']
        colors = ['#E74C3C' if any(k in f.lower() for k in pakistan_keywords)
                 else '#3498DB' for f in top_features['feature']]

        fig = go.Figure(go.Bar(
            x=top_features['importance'],
            y=top_features['feature'],
            orientation='h',
            marker_color=colors
        ))
        fig.update_layout(
            title=f'Top {top_n} Features (Red = Pakistan-specific)',
            height=max(400, top_n * 25),
            xaxis_title='Importance Score'
        )
        st.plotly_chart(fig, use_container_width=True)


# ============================================
# PAGE 4: REORDER ALERTS
# ============================================
elif page == "📦 Reorder Alerts":
    st.markdown('<div class="main-header">📦 Reorder Alert System</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Smart inventory recommendations — preventing stockouts before they happen</div>',
                unsafe_allow_html=True)

    if alerts is not None and len(alerts) > 0:
        # Alert KPIs
        col1, col2, col3, col4 = st.columns(4)

        critical_count = len(alerts[alerts['alert_level'] == 'CRITICAL'])
        high_count = len(alerts[alerts['alert_level'] == 'HIGH'])
        medium_count = len(alerts[alerts['alert_level'] == 'MEDIUM'])
        low_count = len(alerts[alerts['alert_level'] == 'LOW'])

        col1.metric("🔴 Critical", critical_count)
        col2.metric("🟠 High", high_count)
        col3.metric("🟡 Medium", medium_count)
        col4.metric("🟢 Low", low_count)

        st.markdown("---")

        # Alert severity filter
        severity_filter = st.multiselect(
            "Filter by Alert Level",
            ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW'],
            default=['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']
        )

        filtered_alerts = alerts[alerts['alert_level'].isin(severity_filter)]

        # Alert distribution chart
        col_left, col_right = st.columns(2)

        with col_left:
            st.subheader("📊 Alerts by Severity")
            alert_counts = filtered_alerts['alert_level'].value_counts()
            colors_map = {'CRITICAL': '#EF4444', 'HIGH': '#F97316', 'MEDIUM': '#EAB308', 'LOW': '#22C55E'}
            fig = px.bar(x=alert_counts.index, y=alert_counts.values,
                        color=alert_counts.index,
                        color_discrete_map=colors_map,
                        title='Active Alerts by Severity Level')
            fig.update_layout(height=350, showlegend=False,
                            xaxis_title='Alert Level', yaxis_title='Count')
            st.plotly_chart(fig, use_container_width=True)

        with col_right:
            st.subheader("📦 Alerts by Product")
            by_product = filtered_alerts['family'].value_counts().head(10)
            fig = px.pie(values=by_product.values, names=by_product.index,
                        title='Top 10 Products with Active Alerts')
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

        # Alert Table
        st.subheader("📋 Active Reorder Alerts")

        display_cols = [c for c in ['alert_level', 'store_id', 'city', 'family',
                                     'tajir_category', 'avg_daily_demand', 'suggested_order',
                                     'consecutive_zeros', 'alert_reason'] if c in filtered_alerts.columns]

        st.dataframe(
            filtered_alerts[display_cols].sort_values('alert_level'),
            use_container_width=True,
            height=500
        )

        # Download button
        csv_data = filtered_alerts.to_csv(index=False)
        st.download_button(
            label="📥 Download Alerts CSV",
            data=csv_data,
            file_name="reorder_alerts.csv",
            mime="text/csv"
        )

    else:
        st.info("No active alerts. Run `python src/stockout_analysis.py` to generate alerts.")

    # Reorder Recommendations
    if reorder_recs is not None:
        st.markdown("---")
        st.subheader("📊 Reorder Recommendations")

        display_cols = [c for c in ['store_id', 'family', 'city', 'avg_daily_demand',
                                     'reorder_point_95', 'safety_stock_95',
                                     'suggested_order_qty', 'risk_category'] if c in reorder_recs.columns]

        # Filter for non-zero demand
        active_recs = reorder_recs[reorder_recs.get('avg_daily_demand', 0) > 0] if 'avg_daily_demand' in reorder_recs.columns else reorder_recs

        st.dataframe(active_recs[display_cols].head(50), use_container_width=True, height=400)


# ============================================
# PAGE 5: STORE DEEP DIVE
# ============================================
elif page == "🏬 Store Deep Dive":
    st.markdown('<div class="main-header">🏬 Store Deep Dive</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Detailed analysis for individual stores</div>',
                unsafe_allow_html=True)

    # Store selector
    selected_store = st.selectbox("Select a Store", sorted(df['store_id'].unique()))

    store_data = df[df['store_id'] == selected_store]

    if len(store_data) > 0:
        # Store info
        col1, col2, col3, col4, col5 = st.columns(5)

        store_city = store_data['city'].iloc[0] if 'city' in store_data.columns else 'N/A'
        store_type = store_data['store_type'].iloc[0] if 'store_type' in store_data.columns else 'N/A'
        store_revenue = store_data['sales'].sum()
        store_avg = store_data['sales'].mean()
        store_stockout = store_data['is_zero_sale'].mean() * 100 if 'is_zero_sale' in store_data.columns else 0

        col1.metric("📍 City", store_city)
        col2.metric("🏷️ Type", store_type)
        col3.metric("💰 Total Revenue", f"${store_revenue:,.0f}")
        col4.metric("📈 Avg Daily Sales", f"${store_avg:,.1f}")
        col5.metric("🚨 Stockout Rate", f"{store_stockout:.1f}%")

        st.markdown("---")

        # Store sales trend
        col_left, col_right = st.columns(2)

        with col_left:
            st.subheader("📈 Sales Trend")
            monthly = store_data.groupby(store_data['date'].dt.to_period('M').astype(str))['sales'].sum().reset_index()
            monthly.columns = ['Month', 'Sales']
            fig = px.line(monthly, x='Month', y='Sales',
                         title=f'Store {selected_store} Monthly Sales')
            fig.update_layout(xaxis_tickangle=-45, height=400)
            st.plotly_chart(fig, use_container_width=True)

        with col_right:
            st.subheader("📦 Sales by Product")
            by_product = store_data.groupby('family')['sales'].sum().sort_values(ascending=True).tail(10).reset_index()
            by_product.columns = ['Product', 'Sales']
            fig = px.bar(by_product, x='Sales', y='Product', orientation='h',
                        title=f'Store {selected_store} Top Products',
                        color='Sales', color_continuous_scale='Blues')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        # Stockout analysis for this store
        st.subheader(f"🚨 Stockout Analysis for Store {selected_store}")

        if 'is_zero_sale' in store_data.columns:
            by_family = store_data.groupby('family').agg(
                total_days=('sales', 'count'),
                stockout_days=('is_zero_sale', 'sum'),
                avg_sales=('sales', 'mean')
            ).reset_index()
            by_family['stockout_rate'] = (by_family['stockout_days'] / by_family['total_days'] * 100).round(1)
            by_family = by_family.sort_values('stockout_rate', ascending=False)

            fig = px.bar(by_family, x='family', y='stockout_rate',
                        color='stockout_rate', color_continuous_scale='RdYlGn_r',
                        title=f'Stockout Rate by Product — Store {selected_store}')
            fig.update_layout(xaxis_tickangle=-45, height=400,
                            xaxis_title='Product Family', yaxis_title='Stockout Rate (%)')
            st.plotly_chart(fig, use_container_width=True)

        # Store alerts
        if alerts is not None and len(alerts) > 0:
            store_alerts = alerts[alerts['store_id'] == selected_store]
            if len(store_alerts) > 0:
                st.subheader(f"🚨 Active Alerts for Store {selected_store}")
                st.dataframe(store_alerts, use_container_width=True)
            else:
                st.success(f"No active alerts for Store {selected_store}")


# ============================================
# FOOTER
# ============================================
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #888; font-size: 0.85rem;">
    <strong>Tajir Retail — Kiryana Store Demand Forecasting & Inventory Optimization</strong><br>
    Built with Python, SQL Server, XGBoost, and Streamlit<br>
    Data: 2.6M+ records | 54 stores | 33 product families | 2013-2017
</div>
""", unsafe_allow_html=True)