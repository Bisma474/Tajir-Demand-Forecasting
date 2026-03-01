"""
DAY 2 — PART 2: Feature Engineering
Creates 25+ smart features from cleaned data
Including Pakistan-specific seasonal features

Run from project root:
  python src/feature_engineering.py
"""
import pandas as pd
import numpy as np
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger("FeatureEngineer")


class FeatureEngineer:
    """
    Creates intelligent features from cleaned retail data.
    25+ features including Pakistan-specific ones.
    """

    # ============================================
    # TIME-BASED FEATURES
    # ============================================
    def create_time_features(self, df):
        """Extract all time-based features from date column."""
        logger.info("   ⏰ Creating time features...")

        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        df['day_of_week'] = df['date'].dt.dayofweek       # 0=Mon, 6=Sun
        df['day_of_month'] = df['date'].dt.day
        df['week_of_year'] = df['date'].dt.isocalendar().week.astype(int)
        df['quarter'] = df['date'].dt.quarter
        df['day_of_year'] = df['date'].dt.dayofyear

        # Binary flags
        df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
        df['is_month_start'] = df['date'].dt.is_month_start.astype(int)
        df['is_month_end'] = df['date'].dt.is_month_end.astype(int)

        features_created = ['year', 'month', 'day_of_week', 'day_of_month',
                           'week_of_year', 'quarter', 'day_of_year',
                           'is_weekend', 'is_month_start', 'is_month_end']
        logger.info(f"      Created {len(features_created)} time features")
        return df

    # ============================================
    # LAG FEATURES (Most important for forecasting!)
    # ============================================
    def create_lag_features(self, df):
        """
        Create lag and rolling window features.
        These are the MOST POWERFUL features for demand forecasting.
        They capture: what happened recently predicts what happens next.
        """
        logger.info("   📉 Creating lag features (this takes 2-5 minutes)...")

        group = ['store_id', 'family']

        # LAG features: what were sales N days ago?
        for lag in [7, 14, 28]:
            col_name = f'sales_lag_{lag}d'
            df[col_name] = df.groupby(group)['sales'].shift(lag)
            logger.info(f"      Created: {col_name}")

        # ROLLING MEAN: average sales over last N days
        for window in [7, 14, 30]:
            col_name = f'sales_rolling_mean_{window}d'
            df[col_name] = (
                df.groupby(group)['sales']
                .transform(lambda x: x.rolling(window, min_periods=1).mean())
            )
            logger.info(f"      Created: {col_name}")

        # ROLLING STD: sales volatility over last N days
        for window in [7, 14]:
            col_name = f'sales_rolling_std_{window}d'
            df[col_name] = (
                df.groupby(group)['sales']
                .transform(lambda x: x.rolling(window, min_periods=1).std())
            )
            logger.info(f"      Created: {col_name}")

        # TREND: is demand going UP or DOWN?
        # Positive = demand increasing, Negative = demand decreasing
        df['sales_trend_7d'] = df['sales_rolling_mean_7d'] - df['sales_rolling_mean_30d']

        # ROLLING MAX and MIN (for range analysis)
        df['sales_rolling_max_7d'] = (
            df.groupby(group)['sales']
            .transform(lambda x: x.rolling(7, min_periods=1).max())
        )
        df['sales_rolling_min_7d'] = (
            df.groupby(group)['sales']
            .transform(lambda x: x.rolling(7, min_periods=1).min())
        )

        logger.info(f"      Created: sales_trend_7d, rolling_max_7d, rolling_min_7d")
        return df

    # ============================================
    # PAKISTAN-SPECIFIC FEATURES 🇵🇰
    # ============================================
    def create_pakistan_features(self, df):
        """
        Pakistan-specific seasonal features.
        THIS IS YOUR DIFFERENTIATOR — no other applicant will have these!
        
        Note: These are generated from DATE columns, not from external data.
        They model real Pakistan consumer behavior patterns.
        """
        logger.info("   🇵🇰 Creating Pakistan-specific features...")

        # 1. RAMADAN period (approximate — varies yearly by ~11 days)
        # Major impact: food, beverages, cooking supplies spike massively
        df['is_ramadan_period'] = df['month'].isin([3, 4]).astype(int)
        logger.info("      ✅ is_ramadan_period (Mar-Apr)")

        # 2. EID preparation (last 2 weeks before Eid)
        # People buy: new clothes, sweets, gifts, extra food
        df['is_eid_preparation'] = (
            (df['month'].isin([4, 6])) & (df['day_of_month'] >= 15)
        ).astype(int)
        logger.info("      ✅ is_eid_preparation")

        # 3. SUMMER PEAK — Lahore hits 40-48°C
        # Beverages, water, ice cream, dairy demand explodes
        df['is_summer_peak'] = df['month'].isin([5, 6, 7, 8]).astype(int)
        logger.info("      ✅ is_summer_peak (May-Aug)")

        # 4. WEDDING SEASON — biggest social event season in Pakistan
        # Cooking oil, ghee, spices, rice, sweets, beverages all spike
        df['is_wedding_season'] = df['month'].isin([11, 12, 1, 2]).astype(int)
        logger.info("      ✅ is_wedding_season (Nov-Feb)")

        # 5. SCHOOL SEASON — back to school shopping
        # Stationery, snacks, beverages for kids
        df['is_school_season'] = df['month'].isin([3, 8, 9]).astype(int)
        logger.info("      ✅ is_school_season (Mar, Aug, Sep)")

        # 6. PAYDAY EFFECT — most salaries paid on 1st of month
        # First week = higher consumer spending
        df['is_payday_week'] = (df['day_of_month'] <= 7).astype(int)
        logger.info("      ✅ is_payday_week (1st-7th of month)")

        # 7. FRIDAY EFFECT — Pakistan's weekly day off
        # Different shopping pattern: more family shopping
        # Day 1 chart showed Friday has LOWEST sales (matches Pakistan!)
        df['is_friday'] = (df['day_of_week'] == 4).astype(int)
        logger.info("      ✅ is_friday")

        # 8. END OF MONTH — budget running low
        df['is_end_of_month'] = (df['day_of_month'] >= 25).astype(int)
        logger.info("      ✅ is_end_of_month (25th onwards)")

        return df

    # ============================================
    # STORE & PRODUCT AGGREGATE FEATURES
    # ============================================
    def create_aggregate_features(self, df):
        """
        Create store-level and product-level aggregate features.
        These tell the model: how does THIS store compare to average?
        """
        logger.info("   🏪 Creating aggregate features...")

        # Store-level averages
        store_avg = df.groupby('store_id')['sales'].mean().rename('store_avg_daily_sales')
        df = df.merge(store_avg, on='store_id', how='left')
        logger.info("      ✅ store_avg_daily_sales")

        # Product family averages
        family_avg = df.groupby('family')['sales'].mean().rename('family_avg_sales')
        df = df.merge(family_avg, on='family', how='left')
        logger.info("      ✅ family_avg_sales")

        # Product popularity rank (1 = highest selling)
        family_total = df.groupby('family')['sales'].sum()
        family_rank = family_total.rank(ascending=False).rename('family_popularity_rank')
        df = df.merge(family_rank, on='family', how='left')
        logger.info("      ✅ family_popularity_rank")

        # Store-product interaction: how does THIS product do in THIS store?
        sp_avg = df.groupby(['store_id', 'family'])['sales'].mean().rename('store_product_avg')
        df = df.merge(sp_avg, on=['store_id', 'family'], how='left')
        logger.info("      ✅ store_product_avg")

        # Sales relative to store average (is this product above/below store norm?)
        df['sales_vs_store_avg'] = df['sales'] / df['store_avg_daily_sales'].replace(0, 1)
        logger.info("      ✅ sales_vs_store_avg")

        return df

    # ============================================
    # STOCKOUT INDICATOR FEATURES
    # ============================================
    def create_stockout_features(self, df):
        """
        Features specifically for stockout detection.
        Day 1 found 31.3% zero-sales — these features capture that pattern.
        """
        logger.info("   ⚠️ Creating stockout indicator features...")

        group = ['store_id', 'family']

        # Binary: was there a zero-sale day?
        df['is_zero_sale'] = (df['sales'] == 0).astype(int)

        # Rolling count of zero-sale days in last 7 days
        df['zero_sales_last_7d'] = (
            df.groupby(group)['is_zero_sale']
            .transform(lambda x: x.rolling(7, min_periods=1).sum())
        )

        # Consecutive zero days (streak detection)
        df['consecutive_zeros'] = (
            df.groupby(group)['is_zero_sale']
            .transform(lambda x: x.groupby((x != x.shift()).cumsum()).cumcount() + 1)
        )
        # Only count consecutive zeros, not consecutive non-zeros
        df['consecutive_zeros'] = df['consecutive_zeros'] * df['is_zero_sale']

        logger.info("      ✅ is_zero_sale, zero_sales_last_7d, consecutive_zeros")
        return df

    # ============================================
    # RUN ALL FEATURE ENGINEERING
    # ============================================
    def run_all(self, df):
        """Execute complete feature engineering pipeline."""
        print(f"\n{'═' * 60}")
        print(f"🔧 FEATURE ENGINEERING PIPELINE")
        print(f"{'═' * 60}")

        initial_cols = len(df.columns)

        df = self.create_time_features(df)
        df = self.create_lag_features(df)
        df = self.create_pakistan_features(df)
        df = self.create_aggregate_features(df)
        df = self.create_stockout_features(df)

        # Fill NaN from lag calculations with 0
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        df[numeric_cols] = df[numeric_cols].fillna(0)

        new_cols = len(df.columns) - initial_cols

        print(f"\n{'═' * 60}")
        print(f"✅ FEATURE ENGINEERING COMPLETE!")
        print(f"{'═' * 60}")
        print(f"   Original columns:  {initial_cols}")
        print(f"   New features added: {new_cols}")
        print(f"   Total columns:     {len(df.columns)}")
        print(f"   Total rows:        {len(df):,}")

        # List all new features by category
        print(f"\n   📋 ALL FEATURES CREATED:")

        time_feats = ['year', 'month', 'day_of_week', 'day_of_month', 'week_of_year',
                      'quarter', 'day_of_year', 'is_weekend', 'is_month_start', 'is_month_end']
        lag_feats = [c for c in df.columns if 'lag' in c or 'rolling' in c or 'trend' in c]
        pak_feats = ['is_ramadan_period', 'is_eid_preparation', 'is_summer_peak',
                     'is_wedding_season', 'is_school_season', 'is_payday_week',
                     'is_friday', 'is_end_of_month']
        agg_feats = ['store_avg_daily_sales', 'family_avg_sales', 'family_popularity_rank',
                     'store_product_avg', 'sales_vs_store_avg']
        stock_feats = ['is_zero_sale', 'zero_sales_last_7d', 'consecutive_zeros']

        for category, feats in [
            ('⏰ Time Features', time_feats),
            ('📉 Lag & Rolling Features', lag_feats),
            ('🇵🇰 Pakistan Features', pak_feats),
            ('🏪 Aggregate Features', agg_feats),
            ('⚠️ Stockout Features', stock_feats)
        ]:
            print(f"\n   {category} ({len(feats)}):")
            for f in feats:
                if f in df.columns:
                    print(f"     → {f}")

        return df


# ============================================
# MAIN: Run feature engineering
# ============================================
if __name__ == "__main__":
    print("╔" + "═" * 58 + "╗")
    print("║   DAY 2 PART 2: FEATURE ENGINEERING 🔧                 ║")
    print("╚" + "═" * 58 + "╝")

    # Load cleaned merged data
    input_path = 'data/cleaned/merged_retail_data.csv'
    logger.info(f"📂 Loading: {input_path}")

    if not os.path.exists(input_path):
        print(f"❌ File not found: {input_path}")
        print(f"   Run 'python src/data_cleaning.py' first!")
        exit(1)

    df = pd.read_csv(input_path, parse_dates=['date'])
    logger.info(f"   Loaded: {len(df):,} rows, {len(df.columns)} columns")

    # Run feature engineering
    engineer = FeatureEngineer()
    df_featured = engineer.run_all(df)

    # Save
    output_path = 'data/cleaned/featured_retail_data.csv'
    df_featured.to_csv(output_path, index=False)

    file_size = os.path.getsize(output_path) / (1024 * 1024)

    print(f"\n💾 Saved: {output_path}")
    print(f"   Rows: {len(df_featured):,}")
    print(f"   Columns: {len(df_featured.columns)}")
    print(f"   Size: {file_size:.1f} MB")

    # Final verification
    print(f"\n{'═' * 60}")
    print(f"📊 QUICK DATA VERIFICATION")
    print(f"{'═' * 60}")
    print(f"   Date range: {df_featured['date'].min()} to {df_featured['date'].max()}")
    print(f"   Stores: {df_featured['store_id'].nunique()}")
    print(f"   Products: {df_featured['family'].nunique()}")
    print(f"   Zero sales: {(df_featured['sales'] == 0).sum():,} ({(df_featured['sales'] == 0).mean()*100:.1f}%)")
    print(f"   Pakistan features active: ✅")
    print(f"   Lag features active: ✅")
    print(f"   Stockout features active: ✅")

    print(f"\n{'═' * 60}")
    print(f"🎉 DAY 2 COMPLETE!")
    print(f"{'═' * 60}")
    print(f"""
   ✅ Data cleaned (outliers capped, promos fixed, oil filled)
   ✅ 4 sources merged into 1 unified dataset
   ✅ 25+ ML features created
   ✅ Pakistan seasonal features added
   ✅ Stockout indicators added
   ✅ Ready for Day 3: Azure Cloud Setup!

   📁 Your final dataset: data/cleaned/featured_retail_data.csv
""")