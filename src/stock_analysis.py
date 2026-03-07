"""
DAYS 9-10: Stockout Analysis & Reorder Recommendations
This is the BUSINESS VALUE layer that wins the internship.

What it does:
  1. Identifies stockout patterns across all stores
  2. Calculates reorder points for every product-store combo
  3. Assigns risk categories (Critical/High/Medium/Low)
  4. Generates reorder recommendations
  5. Creates charts for dashboard + presentation

Run: python src/stockout_analysis.py
"""
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import os
import pickle
import warnings
warnings.filterwarnings('ignore')

os.makedirs('data/processed', exist_ok=True)
os.makedirs('screenshots', exist_ok=True)


class StockoutAnalyzer:
    """
    Analyzes stockout patterns and generates business insights.
    
    Stockout = when a product has ZERO sales on a day
    This means either:
      a) The store ran out of stock (BAD - lost revenue!)
      b) Nobody wanted it that day (OK - low demand)
    
    We use ML predictions to tell the difference!
    """

    def __init__(self):
        self.df = None
        self.stockout_summary = None
        self.store_risk = None
        self.product_risk = None
        self.results = {}

    # ============================================
    # STEP 1: LOAD DATA
    # ============================================
    def load_data(self):
        """Load ML-ready data + predictions."""
        print("=" * 60)
        print("   STEP 1: LOADING DATA")
        print("=" * 60)

        # Load main data
        if os.path.exists('data/processed/ml_ready_data.csv'):
            path = 'data/processed/ml_ready_data.csv'
        else:
            path = 'data/cleaned/featured_retail_data.csv'

        print(f"   Loading: {path}")
        self.df = pd.read_csv(path, parse_dates=['date'])
        print(f"   Loaded: {len(self.df):,} rows")

        # Load predictions if available
        pred_path = 'data/processed/predictions.csv'
        if os.path.exists(pred_path):
            preds = pd.read_csv(pred_path)
            print(f"   Predictions loaded: {len(preds):,} rows")
            
            # Attach predictions to test period (2017)
            test_mask = self.df['date'].dt.year >= 2017
            test_count = test_mask.sum()
            
            if len(preds) == test_count:
                self.df.loc[test_mask, 'predicted_sales'] = preds['predicted'].values
                print(f"   Predictions attached to {test_count:,} test rows")
            else:
                print(f"   Warning: Prediction count ({len(preds):,}) != test count ({test_count:,})")
                print(f"   Proceeding with actual data only")
                self.df['predicted_sales'] = self.df['sales']
        else:
            print(f"   No predictions file found - using actual sales")
            self.df['predicted_sales'] = self.df['sales']

        # Make sure is_zero_sale column exists
        if 'is_zero_sale' not in self.df.columns:
            self.df['is_zero_sale'] = (self.df['sales'] == 0).astype(int)

        print(f"   Date range: {self.df['date'].min()} to {self.df['date'].max()}")
        
        total_records = len(self.df)
        zero_sales = self.df['is_zero_sale'].sum()
        print(f"   Total records: {total_records:,}")
        print(f"   Zero-sale records: {zero_sales:,} ({zero_sales/total_records*100:.1f}%)")

        return self.df

    # ============================================
    # STEP 2: STOCKOUT PATTERN ANALYSIS
    # ============================================
    def analyze_stockout_patterns(self):
        """
        Identify WHERE, WHEN, and WHAT stockouts happen.
        This answers: "What's the stockout problem at Tajir?"
        """
        print("\n" + "=" * 60)
        print("   STEP 2: STOCKOUT PATTERN ANALYSIS")
        print("=" * 60)

        df = self.df

        # --- 2A: Overall Stockout Statistics ---
        print("\n   [2A] OVERALL STOCKOUT STATISTICS")
        print("   " + "-" * 50)

        total_days = len(df)
        stockout_days = df['is_zero_sale'].sum()
        stockout_rate = stockout_days / total_days * 100

        print(f"   Total store-product-days:  {total_days:>12,}")
        print(f"   Stockout days:             {stockout_days:>12,}")
        print(f"   Overall stockout rate:     {stockout_rate:>11.1f}%")

        # --- 2B: Stockout by Store Type ---
        print("\n   [2B] STOCKOUT BY STORE TYPE")
        print("   " + "-" * 50)

        if 'store_type' in df.columns:
            by_type = df.groupby('store_type').agg(
                total_days=('is_zero_sale', 'count'),
                stockout_days=('is_zero_sale', 'sum'),
                avg_sales=('sales', 'mean')
            ).reset_index()
            by_type['stockout_rate'] = (by_type['stockout_days'] / by_type['total_days'] * 100).round(1)
            by_type = by_type.sort_values('stockout_rate', ascending=False)

            for _, row in by_type.iterrows():
                print(f"   Type {row['store_type']}: {row['stockout_rate']:>5.1f}% stockout | "
                      f"Avg sales: {row['avg_sales']:>8.1f} | "
                      f"Days: {row['total_days']:>10,}")

        # --- 2C: Stockout by Product Category ---
        print("\n   [2C] STOCKOUT BY PRODUCT CATEGORY")
        print("   " + "-" * 50)

        category_col = 'tajir_category' if 'tajir_category' in df.columns else 'family'
        by_product = df.groupby(category_col).agg(
            total_days=('is_zero_sale', 'count'),
            stockout_days=('is_zero_sale', 'sum'),
            avg_sales=('sales', 'mean')
        ).reset_index()
        by_product['stockout_rate'] = (by_product['stockout_days'] / by_product['total_days'] * 100).round(1)
        by_product = by_product.sort_values('stockout_rate', ascending=False)

        for _, row in by_product.head(10).iterrows():
            print(f"   {row[category_col]:<25s}: {row['stockout_rate']:>5.1f}% stockout | "
                  f"Avg sales: {row['avg_sales']:>8.1f}")

        # --- 2D: Stockout by Day of Week ---
        print("\n   [2D] STOCKOUT BY DAY OF WEEK")
        print("   " + "-" * 50)

        if 'day_name' in df.columns:
            by_day = df.groupby('day_name').agg(
                total_days=('is_zero_sale', 'count'),
                stockout_days=('is_zero_sale', 'sum')
            ).reset_index()
            by_day['stockout_rate'] = (by_day['stockout_days'] / by_day['total_days'] * 100).round(1)
            
            day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            by_day['day_order'] = by_day['day_name'].map({d: i for i, d in enumerate(day_order)})
            by_day = by_day.sort_values('day_order')

            for _, row in by_day.iterrows():
                bar = '#' * int(row['stockout_rate'] / 2)
                print(f"   {row['day_name']:<12s}: {row['stockout_rate']:>5.1f}% {bar}")

        # --- 2E: Stockout by Pakistan Seasons ---
        print("\n   [2E] STOCKOUT BY PAKISTAN SEASONS")
        print("   " + "-" * 50)

        season_cols = ['is_ramadan_period', 'is_wedding_season', 'is_summer_peak', 
                       'is_payday_week', 'is_school_season']
        season_names = ['Ramadan Period', 'Wedding Season', 'Summer Peak', 
                        'Payday Week', 'School Season']

        for col, name in zip(season_cols, season_names):
            if col in df.columns:
                in_season = df[df[col] == 1]['is_zero_sale'].mean() * 100
                out_season = df[df[col] == 0]['is_zero_sale'].mean() * 100
                diff = in_season - out_season
                arrow = "UP" if diff > 0 else "DOWN"
                print(f"   {name:<20s}: {in_season:>5.1f}% (vs {out_season:.1f}% normal) [{arrow} {abs(diff):.1f}%]")

        self.results['overall_stockout_rate'] = stockout_rate
        self.results['stockout_days'] = stockout_days

        return by_product

    # ============================================
    # STEP 3: STORE-PRODUCT RISK ASSESSMENT
    # ============================================
    def calculate_risk_scores(self):
        """
        Calculate stockout risk for every store-product combination.
        Assigns risk categories: Critical, High, Medium, Low
        """
        print("\n" + "=" * 60)
        print("   STEP 3: RISK ASSESSMENT")
        print("=" * 60)

        df = self.df

        # Calculate metrics per store-product combination
        print("   Calculating risk scores for every store-product combo...")

        combo_stats = df.groupby(['store_id', 'family']).agg(
            total_days=('sales', 'count'),
            zero_sale_days=('is_zero_sale', 'sum'),
            avg_sales=('sales', 'mean'),
            total_sales=('sales', 'sum'),
            max_consecutive_zeros=('consecutive_zeros', 'max') if 'consecutive_zeros' in df.columns else ('is_zero_sale', 'sum'),
            avg_rolling_mean=('sales_rolling_mean_7d', 'mean') if 'sales_rolling_mean_7d' in df.columns else ('sales', 'mean'),
            sales_volatility=('sales_rolling_std_7d', 'mean') if 'sales_rolling_std_7d' in df.columns else ('sales', 'std')
        ).reset_index()

        # Stockout rate
        combo_stats['stockout_rate'] = (combo_stats['zero_sale_days'] / combo_stats['total_days'] * 100).round(2)

        # Revenue impact (how much revenue is lost when stocked out)
        overall_avg = df['sales'].mean()
        combo_stats['revenue_impact'] = (combo_stats['avg_sales'] * combo_stats['zero_sale_days']).round(0)

        # RISK SCORE (0-100)
        # Higher score = more critical to fix
        combo_stats['risk_score'] = (
            combo_stats['stockout_rate'] * 0.3 +                          # 30% weight: how often stockout
            (combo_stats['avg_sales'] / combo_stats['avg_sales'].max() * 100) * 0.3 +  # 30% weight: high-selling product
            (combo_stats['max_consecutive_zeros'] / combo_stats['max_consecutive_zeros'].max() * 100) * 0.2 +  # 20%: consecutive zeros
            (combo_stats['sales_volatility'] / combo_stats['sales_volatility'].max() * 100) * 0.2  # 20%: volatility
        ).round(1)

        # Risk categories
        combo_stats['risk_category'] = pd.cut(
            combo_stats['risk_score'],
            bins=[0, 25, 50, 75, 100],
            labels=['Low', 'Medium', 'High', 'Critical']
        )

        # Add store and product info
        if 'city' in df.columns:
            city_map = df.groupby('store_id')['city'].first().to_dict()
            combo_stats['city'] = combo_stats['store_id'].map(city_map)

        if 'store_type' in df.columns:
            type_map = df.groupby('store_id')['store_type'].first().to_dict()
            combo_stats['store_type'] = combo_stats['store_id'].map(type_map)

        if 'tajir_category' in df.columns:
            cat_map = df.groupby('family')['tajir_category'].first().to_dict()
            combo_stats['tajir_category'] = combo_stats['family'].map(cat_map)

        if 'is_fmcg' in df.columns:
            fmcg_map = df.groupby('family')['is_fmcg'].first().to_dict()
            combo_stats['is_fmcg'] = combo_stats['family'].map(fmcg_map)

        self.stockout_summary = combo_stats.sort_values('risk_score', ascending=False)

        # Print risk summary
        risk_counts = combo_stats['risk_category'].value_counts().sort_index(ascending=False)
        print(f"\n   RISK CATEGORY DISTRIBUTION:")
        print(f"   " + "-" * 50)
        for category, count in risk_counts.items():
            pct = count / len(combo_stats) * 100
            bar = '#' * int(pct)
            print(f"   {str(category):<12s}: {count:>6,} combos ({pct:>5.1f}%) {bar}")

        print(f"\n   TOTAL store-product combinations: {len(combo_stats):,}")

        # Top 20 most critical
        print(f"\n   TOP 20 MOST CRITICAL STOCKOUT RISKS:")
        print(f"   " + "-" * 80)
        print(f"   {'Store':>5s} | {'City':<15s} | {'Product':<25s} | {'Rate':>6s} | {'Score':>5s} | {'Risk':<10s}")
        print(f"   " + "-" * 80)

        for _, row in self.stockout_summary.head(20).iterrows():
            city = str(row.get('city', 'N/A'))[:15]
            print(f"   {int(row['store_id']):>5d} | {city:<15s} | {row['family']:<25s} | "
                  f"{row['stockout_rate']:>5.1f}% | {row['risk_score']:>5.1f} | {str(row['risk_category']):<10s}")

        # Save
        self.stockout_summary.to_csv('data/processed/stockout_risk_assessment.csv', index=False)
        print(f"\n   Saved: data/processed/stockout_risk_assessment.csv")

        return self.stockout_summary

    # ============================================
    # STEP 4: REORDER POINT CALCULATION
    # ============================================
    def calculate_reorder_points(self):
        """
        Calculate optimal reorder point for every store-product combo.
        
        Reorder Point = (Average Daily Demand x Lead Time) + Safety Stock
        Safety Stock = Z-score x Std Dev of Demand x sqrt(Lead Time)
        
        This is what Tajir's ordering system would actually use!
        """
        print("\n" + "=" * 60)
        print("   STEP 4: REORDER POINT CALCULATION")
        print("=" * 60)

        df = self.df

        # Calculate per store-product
        reorder = df.groupby(['store_id', 'family']).agg(
            avg_daily_demand=('sales', 'mean'),
            std_daily_demand=('sales', 'std'),
            max_daily_demand=('sales', 'max'),
            median_demand=('sales', 'median'),
            total_days=('sales', 'count')
        ).reset_index()

        # Parameters (Tajir Pakistan context)
        LEAD_TIME_DAYS = 2          # Typical kiryana store restocking takes 1-2 days
        Z_SCORE_95 = 1.65           # 95% service level (industry standard)
        Z_SCORE_99 = 2.33           # 99% service level (for critical items)
        ORDER_CYCLE_DAYS = 7        # Weekly ordering

        # Safety stock calculation
        reorder['safety_stock_95'] = (
            Z_SCORE_95 * reorder['std_daily_demand'] * np.sqrt(LEAD_TIME_DAYS)
        ).round(0)

        reorder['safety_stock_99'] = (
            Z_SCORE_99 * reorder['std_daily_demand'] * np.sqrt(LEAD_TIME_DAYS)
        ).round(0)

        # Reorder point = (avg demand x lead time) + safety stock
        reorder['reorder_point_95'] = (
            reorder['avg_daily_demand'] * LEAD_TIME_DAYS + reorder['safety_stock_95']
        ).round(0)

        reorder['reorder_point_99'] = (
            reorder['avg_daily_demand'] * LEAD_TIME_DAYS + reorder['safety_stock_99']
        ).round(0)

        # Order quantity (EOQ simplified) = average weekly demand
        reorder['suggested_order_qty'] = (
            reorder['avg_daily_demand'] * ORDER_CYCLE_DAYS
        ).round(0)

        # Maximum stock level
        reorder['max_stock_level'] = (
            reorder['suggested_order_qty'] + reorder['safety_stock_95']
        ).round(0)

        # Add risk info from stockout analysis
        if self.stockout_summary is not None:
            risk_map = self.stockout_summary.set_index(['store_id', 'family'])[
                ['stockout_rate', 'risk_score', 'risk_category']
            ]
            reorder = reorder.set_index(['store_id', 'family']).join(risk_map).reset_index()

        # Add store and product info
        if 'city' in df.columns:
            city_map = df.groupby('store_id')['city'].first().to_dict()
            reorder['city'] = reorder['store_id'].map(city_map)

        if 'tajir_category' in df.columns:
            cat_map = df.groupby('family')['tajir_category'].first().to_dict()
            reorder['tajir_category'] = reorder['family'].map(cat_map)

        if 'is_fmcg' in df.columns:
            fmcg_map = df.groupby('family')['is_fmcg'].first().to_dict()
            reorder['is_fmcg'] = reorder['family'].map(fmcg_map)

        # Sort by risk
        reorder = reorder.sort_values('risk_score', ascending=False)

        # Print sample
        print(f"\n   REORDER RECOMMENDATIONS (Top 20 Critical):")
        print(f"   " + "-" * 95)
        print(f"   {'Store':>5s} | {'Product':<22s} | {'Avg/Day':>7s} | {'Reorder Pt':>10s} | "
              f"{'Safety Stk':>10s} | {'Order Qty':>9s} | {'Risk':<10s}")
        print(f"   " + "-" * 95)

        for _, row in reorder.head(20).iterrows():
            print(f"   {int(row['store_id']):>5d} | {row['family']:<22s} | "
                  f"{row['avg_daily_demand']:>7.1f} | {row['reorder_point_95']:>10.0f} | "
                  f"{row['safety_stock_95']:>10.0f} | {row['suggested_order_qty']:>9.0f} | "
                  f"{str(row.get('risk_category', 'N/A')):<10s}")

        # Save
        reorder.to_csv('data/processed/reorder_recommendations.csv', index=False)
        print(f"\n   Saved: data/processed/reorder_recommendations.csv")
        print(f"   Total combos: {len(reorder):,}")

        self.reorder_data = reorder
        return reorder

    # ============================================
    # STEP 5: GENERATE REORDER ALERTS
    # ============================================
    def generate_alerts(self):
        """
        Generate ACTIVE alerts for products that need reordering RIGHT NOW.
        This simulates what Tajir's app would show store owners.
        """
        print("\n" + "=" * 60)
        print("   STEP 5: ACTIVE REORDER ALERTS")
        print("=" * 60)

        df = self.df

        # Get the latest date in data
        latest_date = df['date'].max()
        print(f"   Alert date: {latest_date}")

        # Get latest records for each store-product
        latest = df[df['date'] == latest_date].copy()

        if len(latest) == 0:
            # Try last 7 days
            recent_cutoff = latest_date - pd.Timedelta(days=7)
            latest = df[df['date'] >= recent_cutoff].copy()
            latest = latest.sort_values('date').groupby(['store_id', 'family']).last().reset_index()

        print(f"   Latest records: {len(latest):,}")

        # ALERT CONDITIONS:
        # 1. CRITICAL: consecutive_zeros >= 3 AND product is FMCG AND normally sells well
        # 2. HIGH: consecutive_zeros >= 2 AND avg demand > 10
        # 3. MEDIUM: stockout today AND predicted demand > avg
        # 4. LOW: stockout today for any product

        alerts = []

        for _, row in latest.iterrows():
            consec = row.get('consecutive_zeros', 0)
            is_fmcg = row.get('is_fmcg', 0)
            avg_demand = row.get('sales_rolling_mean_7d', row.get('sales', 0))
            predicted = row.get('predicted_sales', avg_demand)
            is_stockout = row.get('is_zero_sale', 0)

            alert_level = None
            alert_reason = ""

            if consec >= 3 and is_fmcg == 1 and avg_demand > 10:
                alert_level = 'CRITICAL'
                alert_reason = f"{int(consec)} consecutive zero-sale days for high-demand FMCG product"
            elif consec >= 2 and avg_demand > 10:
                alert_level = 'HIGH'
                alert_reason = f"{int(consec)} consecutive zero-sale days, avg demand: {avg_demand:.0f}"
            elif is_stockout == 1 and predicted > avg_demand * 1.2:
                alert_level = 'MEDIUM'
                alert_reason = f"Stockout today, predicted demand ({predicted:.0f}) above average ({avg_demand:.0f})"
            elif is_stockout == 1 and avg_demand > 5:
                alert_level = 'LOW'
                alert_reason = f"Stockout today for product with avg demand {avg_demand:.0f}"

            if alert_level:
                alerts.append({
                    'store_id': int(row['store_id']),
                    'family': row['family'],
                    'city': row.get('city', 'Unknown'),
                    'tajir_category': row.get('tajir_category', 'Unknown'),
                    'alert_level': alert_level,
                    'alert_reason': alert_reason,
                    'consecutive_zeros': int(consec),
                    'avg_daily_demand': round(float(avg_demand), 1),
                    'predicted_demand': round(float(predicted), 1),
                    'suggested_order': round(float(avg_demand * 7), 0),
                    'date': str(latest_date.date()) if hasattr(latest_date, 'date') else str(latest_date)
                })

        alerts_df = pd.DataFrame(alerts)

        if len(alerts_df) > 0:
            # Sort by severity
            severity_order = {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2, 'LOW': 3}
            alerts_df['severity_rank'] = alerts_df['alert_level'].map(severity_order)
            alerts_df = alerts_df.sort_values(['severity_rank', 'avg_daily_demand'], ascending=[True, False])
            alerts_df = alerts_df.drop(columns=['severity_rank'])

            # Summary
            print(f"\n   ALERT SUMMARY:")
            print(f"   " + "-" * 50)
            alert_counts = alerts_df['alert_level'].value_counts()
            for level in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']:
                count = alert_counts.get(level, 0)
                bar = '!' * min(count, 50)
                print(f"   {level:<10s}: {count:>6,} alerts {bar}")

            print(f"\n   Total alerts: {len(alerts_df):,}")

            # Top 15 critical alerts
            print(f"\n   TOP 15 CRITICAL ALERTS:")
            print(f"   " + "-" * 90)
            print(f"   {'Level':<10s} | {'Store':>5s} | {'City':<12s} | {'Product':<22s} | "
                  f"{'Demand':>6s} | {'Order':>5s} | Reason")
            print(f"   " + "-" * 90)

            for _, row in alerts_df.head(15).iterrows():
                city = str(row.get('city', 'N/A'))[:12]
                print(f"   {row['alert_level']:<10s} | {row['store_id']:>5d} | {city:<12s} | "
                      f"{row['family']:<22s} | {row['avg_daily_demand']:>6.0f} | "
                      f"{row['suggested_order']:>5.0f} | {row['alert_reason'][:40]}")

            # Save
            alerts_df.to_csv('data/processed/active_reorder_alerts.csv', index=False)
            print(f"\n   Saved: data/processed/active_reorder_alerts.csv")
        else:
            print("   No active alerts generated")
            alerts_df = pd.DataFrame()

        self.alerts = alerts_df
        return alerts_df

    # ============================================
    # STEP 6: GENERATE ALL CHARTS
    # ============================================
    def generate_charts(self):
        """Generate stockout analysis and reorder charts."""
        print("\n" + "=" * 60)
        print("   STEP 6: GENERATING CHARTS")
        print("=" * 60)

        df = self.df

        # ---- CHART 1: Stockout Overview (4 subplots) ----
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Stockout Analysis - Tajir Retail\nIdentifying Where Revenue is Being Lost',
                     fontsize=16, fontweight='bold')

        # 1A: Stockout rate by store type
        if 'store_type' in df.columns:
            by_type = df.groupby('store_type')['is_zero_sale'].mean() * 100
            by_type.sort_values(ascending=True).plot(kind='barh', ax=axes[0, 0], color='#E74C3C')
            axes[0, 0].set_title('Stockout Rate by Store Type')
            axes[0, 0].set_xlabel('Stockout Rate (%)')
            axes[0, 0].axvline(x=df['is_zero_sale'].mean()*100, color='black', 
                              linestyle='--', label=f'Average ({df["is_zero_sale"].mean()*100:.1f}%)')
            axes[0, 0].legend()

        # 1B: Stockout rate by product family (top 15)
        by_family = df.groupby('family')['is_zero_sale'].mean() * 100
        by_family.sort_values(ascending=True).tail(15).plot(
            kind='barh', ax=axes[0, 1], color='#E67E22')
        axes[0, 1].set_title('Top 15 Products by Stockout Rate')
        axes[0, 1].set_xlabel('Stockout Rate (%)')

        # 1C: Stockout rate by month
        by_month = df.groupby('month')['is_zero_sale'].mean() * 100
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                       'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        by_month.index = [month_names[i-1] for i in by_month.index]
        by_month.plot(kind='bar', ax=axes[1, 0], color='#3498DB')
        axes[1, 0].set_title('Stockout Rate by Month (Seasonal Pattern)')
        axes[1, 0].set_ylabel('Stockout Rate (%)')
        axes[1, 0].tick_params(axis='x', rotation=45)

        # 1D: Risk category distribution
        if self.stockout_summary is not None:
            risk_counts = self.stockout_summary['risk_category'].value_counts()
            colors_pie = ['#E74C3C', '#E67E22', '#F1C40F', '#2ECC71']
            risk_counts.plot(kind='pie', ax=axes[1, 1], colors=colors_pie,
                           autopct='%1.1f%%', startangle=90)
            axes[1, 1].set_title('Risk Category Distribution')
            axes[1, 1].set_ylabel('')

        plt.tight_layout()
        plt.savefig('screenshots/day9_stockout_analysis.png', dpi=150, bbox_inches='tight')
        print(f"   Saved: screenshots/day9_stockout_analysis.png")

        # ---- CHART 2: Reorder Analysis ----
        fig2, axes2 = plt.subplots(2, 2, figsize=(16, 12))
        fig2.suptitle('Reorder Alert System - Tajir Retail\nSmart Inventory Recommendations',
                      fontsize=16, fontweight='bold')

        # 2A: Alert distribution
        if hasattr(self, 'alerts') and len(self.alerts) > 0:
            alert_counts = self.alerts['alert_level'].value_counts()
            colors_alert = {'CRITICAL': '#E74C3C', 'HIGH': '#E67E22', 'MEDIUM': '#F1C40F', 'LOW': '#2ECC71'}
            alert_colors = [colors_alert.get(level, '#95A5A6') for level in alert_counts.index]
            alert_counts.plot(kind='bar', ax=axes2[0, 0], color=alert_colors)
            axes2[0, 0].set_title('Active Alerts by Severity')
            axes2[0, 0].set_ylabel('Number of Alerts')
            axes2[0, 0].tick_params(axis='x', rotation=0)

        # 2B: Top 10 products needing reorder
        if hasattr(self, 'alerts') and len(self.alerts) > 0:
            top_products = self.alerts.groupby('family')['suggested_order'].sum().sort_values(ascending=True).tail(10)
            top_products.plot(kind='barh', ax=axes2[0, 1], color='#9B59B6')
            axes2[0, 1].set_title('Top 10 Products by Reorder Volume')
            axes2[0, 1].set_xlabel('Total Suggested Order Quantity')

        # 2C: Stockout rate vs Average Sales (scatter)
        if self.stockout_summary is not None:
            sample = self.stockout_summary.head(500)
            scatter = axes2[1, 0].scatter(
                sample['avg_sales'], sample['stockout_rate'],
                c=sample['risk_score'], cmap='RdYlGn_r',
                alpha=0.6, s=30
            )
            plt.colorbar(scatter, ax=axes2[1, 0], label='Risk Score')
            axes2[1, 0].set_title('Stockout Rate vs Average Sales')
            axes2[1, 0].set_xlabel('Average Daily Sales')
            axes2[1, 0].set_ylabel('Stockout Rate (%)')
            axes2[1, 0].grid(True, alpha=0.3)

        # 2D: Pakistan seasonal impact on stockouts
        season_data = {}
        for col, name in [('is_ramadan_period', 'Ramadan'), ('is_wedding_season', 'Wedding'),
                          ('is_summer_peak', 'Summer'), ('is_payday_week', 'Payday'),
                          ('is_school_season', 'School')]:
            if col in df.columns:
                season_data[name] = df[df[col] == 1]['is_zero_sale'].mean() * 100

        if season_data:
            normal_rate = df['is_zero_sale'].mean() * 100
            seasons = pd.Series(season_data).sort_values(ascending=True)
            colors_season = ['#E74C3C' if v > normal_rate else '#2ECC71' for v in seasons.values]
            seasons.plot(kind='barh', ax=axes2[1, 1], color=colors_season)
            axes2[1, 1].axvline(x=normal_rate, color='black', linestyle='--',
                               label=f'Normal ({normal_rate:.1f}%)')
            axes2[1, 1].set_title('Stockout Rate During Pakistan Seasons')
            axes2[1, 1].set_xlabel('Stockout Rate (%)')
            axes2[1, 1].legend()

        plt.tight_layout()
        plt.savefig('screenshots/day9_reorder_alerts.png', dpi=150, bbox_inches='tight')
        print(f"   Saved: screenshots/day9_reorder_alerts.png")

        # ---- CHART 3: Store-level heatmap ----
        fig3, ax3 = plt.subplots(1, 1, figsize=(16, 10))

        if 'store_type' in df.columns:
            # Stockout rate by store and product category
            cat_col = 'tajir_category' if 'tajir_category' in df.columns else 'family'
            heatmap_data = df.groupby(['store_id', cat_col])['is_zero_sale'].mean() * 100
            heatmap_pivot = heatmap_data.unstack(fill_value=0)
            
            # Take top 20 stores by stockout rate for readability
            top_stores = df.groupby('store_id')['is_zero_sale'].mean().sort_values(ascending=False).head(20).index
            heatmap_pivot = heatmap_pivot.loc[heatmap_pivot.index.isin(top_stores)]

            sns.heatmap(heatmap_pivot, annot=True, fmt='.0f', cmap='RdYlGn_r',
                       ax=ax3, linewidths=0.5, cbar_kws={'label': 'Stockout Rate (%)'})
            ax3.set_title('Stockout Heatmap: Top 20 At-Risk Stores x Product Category', fontsize=14)
            ax3.set_ylabel('Store ID')
            ax3.set_xlabel('Product Category')

        plt.tight_layout()
        plt.savefig('screenshots/day9_stockout_heatmap.png', dpi=150, bbox_inches='tight')
        print(f"   Saved: screenshots/day9_stockout_heatmap.png")


# ============================================
# MAIN
# ============================================
if __name__ == "__main__":
    print("=" * 60)
    print("   DAYS 9-10: STOCKOUT ANALYSIS + REORDER ALERTS")
    print("   Building the business value layer")
    print("=" * 60)

    analyzer = StockoutAnalyzer()

    # Step 1: Load data
    analyzer.load_data()

    # Step 2: Analyze stockout patterns
    analyzer.analyze_stockout_patterns()

    # Step 3: Calculate risk scores
    analyzer.calculate_risk_scores()

    # Step 4: Calculate reorder points
    analyzer.calculate_reorder_points()

    # Step 5: Generate active alerts
    analyzer.generate_alerts()

    # Step 6: Generate charts
    analyzer.generate_charts()

    # Final summary
    total_combos = len(analyzer.stockout_summary) if analyzer.stockout_summary is not None else 0
    total_alerts = len(analyzer.alerts) if hasattr(analyzer, 'alerts') else 0
    critical_alerts = len(analyzer.alerts[analyzer.alerts['alert_level'] == 'CRITICAL']) if total_alerts > 0 else 0

    print(f"\n{'=' * 60}")
    print(f"   DAYS 9-10 COMPLETE!")
    print(f"{'=' * 60}")
    print(f"""
   STOCKOUT ANALYSIS RESULTS:
      Overall stockout rate:    {analyzer.results.get('overall_stockout_rate', 0):.1f}%
      Total stockout days:      {analyzer.results.get('stockout_days', 0):,}
      Store-product combos:     {total_combos:,}

   REORDER ALERT SYSTEM:
      Total active alerts:      {total_alerts:,}
      CRITICAL alerts:          {critical_alerts:,}

   FILES SAVED:
      data/processed/stockout_risk_assessment.csv
      data/processed/reorder_recommendations.csv
      data/processed/active_reorder_alerts.csv
      screenshots/day9_stockout_analysis.png
      screenshots/day9_reorder_alerts.png
      screenshots/day9_stockout_heatmap.png

   CHECKLIST:
      [x] Stockout patterns identified across all stores
      [x] Reorder points calculated for every product-store combo
      [x] Risk categories assigned (Critical/High/Medium/Low)
      [x] Reorder recommendations CSV generated
      [x] Active alerts generated with severity levels
      [x] 3 analysis charts saved for dashboard

   NEXT: Day 11-12 - Build Streamlit Dashboard!
""")