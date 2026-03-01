"""
DAY 2 — PART 1: Data Cleaning & Merging
Fixes ALL issues found in Day 1 exploration
Merges 4 Kaggle sources into ONE clean dataset

Run from project root:
  python src/data_cleaning.py
"""
import pandas as pd
import numpy as np
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger("DataCleaner")


class RetailDataCleaner:
    """
    Complete data cleaning pipeline for retail sales data.
    Handles every issue found in Day 1 exploration.
    """

    def __init__(self):
        self.cleaning_report = {}

    # ============================================
    # CLEAN SALES DATA (train.csv)
    # ============================================
    def clean_sales(self, df):
        """
        Clean the main sales dataset.
        Issues found in Day 1:
          - onpromotion is counts not binary (260% was wrong)
          - Extreme outliers (max=124,717 vs median=11)
          - Zero sales = 31.3% (KEEP as stockout signal)
          - No nulls, no negatives (already clean)
        """
        logger.info("🧹 Cleaning sales data...")
        initial_rows = len(df)

        # 1. Remove exact duplicate rows
        df = df.drop_duplicates()
        dupes_removed = initial_rows - len(df)
        logger.info(f"   Duplicates removed: {dupes_removed}")

        # 2. Ensure date is datetime
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        date_nulls = df['date'].isnull().sum()
        df = df.dropna(subset=['date'])
        logger.info(f"   Invalid dates dropped: {date_nulls}")

        # 3. FIX: Convert onpromotion from COUNTS to BINARY (0/1)
        # Day 1 showed "260% promotion" — that was wrong because
        # onpromotion had values like 0, 1, 5, 10 (number of items on promo)
        promo_before = df['onpromotion'].describe()
        df['onpromotion_count'] = df['onpromotion']  # keep original as feature
        df['onpromotion'] = (df['onpromotion'] > 0).astype(int)  # binary flag
        promo_pct = df['onpromotion'].mean() * 100
        logger.info(f"   ✅ onpromotion fixed: converted counts → binary")
        logger.info(f"      Original max count: {promo_before['max']:.0f}")
        logger.info(f"      Actual promotion rate: {promo_pct:.1f}% of rows")

        # 4. Handle negative sales (Day 1 showed 0, but safety check)
        negatives = (df['sales'] < 0).sum()
        if negatives > 0:
            df['sales'] = df['sales'].clip(lower=0)
            logger.info(f"   Negative sales clipped: {negatives}")
        else:
            logger.info(f"   Negative sales: 0 (already clean ✅)")

        # 5. Cap EXTREME outliers at 99.5th percentile
        # Day 1: max=124,717 but median=11 — hugely skewed
        upper_cap = df['sales'].quantile(0.995)
        outliers_count = (df['sales'] > upper_cap).sum()
        df['sales'] = df['sales'].clip(upper=upper_cap)
        logger.info(f"   Outliers capped: {outliers_count:,} rows above {upper_cap:,.0f}")

        # 6. Investigate 2015 anomaly (mysterious dip seen in chart)
        # Check if there are dates with unusually low total sales
        daily_totals = df.groupby('date')['sales'].sum()
        anomaly_dates = daily_totals[daily_totals < daily_totals.quantile(0.01)]
        logger.info(f"   Anomaly dates (bottom 1% sales): {len(anomaly_dates)} days")
        if len(anomaly_dates) > 0:
            logger.info(f"   Worst dates: {anomaly_dates.head(5).index.tolist()}")
            # Flag these but DON'T remove — ML model should learn from them
            df['is_anomaly_day'] = df['date'].isin(anomaly_dates.index).astype(int)
            logger.info(f"   Anomaly flag added (not removed — model will learn)")

        # 7. Standardize text columns
        df['family'] = df['family'].str.strip().str.upper()

        # 8. Sort chronologically
        df = df.sort_values(['store_nbr', 'family', 'date']).reset_index(drop=True)

        # Save report
        self.cleaning_report['sales'] = {
            'initial_rows': initial_rows,
            'final_rows': len(df),
            'duplicates_removed': dupes_removed,
            'outliers_capped': outliers_count,
            'outlier_threshold': round(upper_cap, 2),
            'actual_promo_rate': f"{promo_pct:.1f}%",
            'zero_sales_kept': int((df['sales'] == 0).sum()),
            'anomaly_days_flagged': len(anomaly_dates) if len(anomaly_dates) > 0 else 0
        }

        logger.info(f"   ✅ Sales cleaned: {initial_rows:,} → {len(df):,} rows")
        return df

    # ============================================
    # CLEAN STORES DATA (stores.csv)
    # ============================================
    def clean_stores(self, df):
        """
        Clean store metadata.
        Day 1 finding: Ecuador cities — we'll keep them but map to Tajir context.
        """
        logger.info("🧹 Cleaning stores data...")

        df = df.drop_duplicates(subset=['store_nbr'])
        df['city'] = df['city'].str.strip().str.title()
        df['state'] = df['state'].str.strip().str.title()
        df['type'] = df['type'].str.strip().str.upper()

        # Map store types to Tajir-friendly names
        df['store_size'] = df['type'].map({
            'A': 'Large',    # Avg daily sales ~710 (from Day 1)
            'B': 'Medium-Large',  # ~330
            'C': 'Small',    # ~195
            'D': 'Medium',   # ~370
            'E': 'Medium-Small'   # ~280
        })

        self.cleaning_report['stores'] = {
            'total_stores': len(df),
            'store_types': df['type'].value_counts().to_dict(),
            'cities': df['city'].nunique()
        }

        logger.info(f"   ✅ Stores cleaned: {len(df)} stores")
        return df

    # ============================================
    # CLEAN HOLIDAYS DATA (holidays_events.csv)
    # ============================================
    def clean_holidays(self, df):
        """
        Clean holidays data.
        Day 1 finding: Ecuador-specific holidays. Keep them as
        "holiday flag" since sales patterns around ANY holiday are similar.
        """
        logger.info("🧹 Cleaning holidays data...")

        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['date'])
        df['type'] = df['type'].str.strip().str.lower()
        df['description'] = df['description'].str.strip()

        # Remove transferred holidays (avoid double-counting)
        before = len(df)
        df = df[df['transferred'] == False]
        logger.info(f"   Transferred holidays removed: {before - len(df)}")

        # Create simplified holiday flag (national/regional/local)
        df['is_national'] = df['locale'].str.lower().str.contains('national', na=False).astype(int)

        self.cleaning_report['holidays'] = {
            'total_events': len(df),
            'types': df['type'].value_counts().to_dict()
        }

        logger.info(f"   ✅ Holidays cleaned: {len(df)} events")
        return df

    # ============================================
    # CLEAN OIL DATA (oil.csv)
    # ============================================
    def clean_oil(self, df):
        """
        Clean oil price data.
        Day 1 finding: 43 null values (3.5%) — weekends/holidays have no trading.
        """
        logger.info("🧹 Cleaning oil data...")

        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['date'])

        # Forward fill nulls (weekend price = last trading day price)
        null_before = df['dcoilwtico'].isnull().sum()
        df['dcoilwtico'] = df['dcoilwtico'].ffill()
        df['dcoilwtico'] = df['dcoilwtico'].bfill()  # handle very first rows
        null_after = df['dcoilwtico'].isnull().sum()

        self.cleaning_report['oil'] = {
            'total_days': len(df),
            'nulls_filled': null_before - null_after,
            'price_range': f"${df['dcoilwtico'].min():.2f} - ${df['dcoilwtico'].max():.2f}"
        }

        logger.info(f"   Nulls filled: {null_before} → {null_after}")
        logger.info(f"   ✅ Oil data cleaned: {len(df)} days")
        return df

    # ============================================
    # MERGE ALL SOURCES INTO ONE DATASET
    # ============================================
    def merge_all(self, sales, stores, holidays, oil):
        """
        Merge all 4 cleaned sources into ONE unified dataset.
        This is the core data engineering task.
        """
        logger.info("🔗 MERGING ALL DATA SOURCES...")

        # MERGE 1: Sales + Stores (add store metadata)
        merged = sales.merge(
            stores[['store_nbr', 'city', 'state', 'type', 'cluster', 'store_size']],
            on='store_nbr',
            how='left'
        )
        logger.info(f"   After store merge: {len(merged):,} rows, {len(merged.columns)} cols")

        # MERGE 2: Add holiday flag
        # Create a date-level holiday flag (1 if any holiday on that date)
        holiday_dates = holidays.groupby('date').agg(
            is_holiday=('type', 'count'),
            is_national_holiday=('is_national', 'max')
        ).reset_index()
        holiday_dates['is_holiday'] = 1  # any event = holiday flag
        
        merged = merged.merge(holiday_dates[['date', 'is_holiday', 'is_national_holiday']], 
                              on='date', how='left')
        merged['is_holiday'] = merged['is_holiday'].fillna(0).astype(int)
        merged['is_national_holiday'] = merged['is_national_holiday'].fillna(0).astype(int)
        logger.info(f"   After holiday merge: {len(merged):,} rows, {len(merged.columns)} cols")

        # MERGE 3: Add oil prices
        merged = merged.merge(oil[['date', 'dcoilwtico']], on='date', how='left')
        merged['dcoilwtico'] = merged['dcoilwtico'].ffill().bfill()
        merged = merged.rename(columns={'dcoilwtico': 'oil_price'})
        logger.info(f"   After oil merge: {len(merged):,} rows, {len(merged.columns)} cols")

        # Rename for consistency
        merged = merged.rename(columns={
            'store_nbr': 'store_id',
            'type': 'store_type'
        })

        # Final null check
        null_summary = merged.isnull().sum()
        has_nulls = null_summary[null_summary > 0]
        if len(has_nulls) > 0:
            logger.info(f"   Remaining nulls:")
            for col, count in has_nulls.items():
                logger.info(f"      {col}: {count}")
                # Fill numeric nulls with 0
                if merged[col].dtype in ['float64', 'int64']:
                    merged[col] = merged[col].fillna(0)
        
        logger.info(f"   ✅ MERGE COMPLETE: {len(merged):,} rows, {len(merged.columns)} columns")

        self.cleaning_report['merged'] = {
            'final_rows': len(merged),
            'final_columns': len(merged.columns),
            'column_list': merged.columns.tolist()
        }

        return merged

    # ============================================
    # PRINT FULL CLEANING REPORT
    # ============================================
    def print_report(self):
        """Print professional cleaning report."""
        print(f"\n{'═' * 60}")
        print(f"📊 DATA CLEANING REPORT")
        print(f"{'═' * 60}")

        for source, stats in self.cleaning_report.items():
            print(f"\n  📁 {source.upper()}:")
            for key, value in stats.items():
                if isinstance(value, list):
                    print(f"     {key}:")
                    for item in value[:10]:
                        print(f"       → {item}")
                    if len(value) > 10:
                        print(f"       ... and {len(value) - 10} more")
                elif isinstance(value, dict):
                    print(f"     {key}:")
                    for k, v in value.items():
                        print(f"       {k}: {v}")
                else:
                    print(f"     {key}: {value:,}" if isinstance(value, int) else f"     {key}: {value}")

        print(f"\n{'═' * 60}")


# ============================================
# MAIN: Run the complete cleaning pipeline
# ============================================
if __name__ == "__main__":
    print("╔" + "═" * 58 + "╗")
    print("║     DAY 2 PART 1: DATA CLEANING & MERGING 🧹           ║")
    print("╚" + "═" * 58 + "╝")

    cleaner = RetailDataCleaner()

    # Load raw data
    logger.info("📂 Loading raw data...")
    sales = pd.read_csv('data/raw/train.csv', parse_dates=['date'])
    stores = pd.read_csv('data/raw/stores.csv')
    holidays = pd.read_csv('data/raw/holidays_events.csv', parse_dates=['date'])
    oil = pd.read_csv('data/raw/oil.csv', parse_dates=['date'])

    logger.info(f"   Loaded: {len(sales):,} sales + {len(stores)} stores + "
                f"{len(holidays)} holidays + {len(oil)} oil prices")

    # Clean each source
    sales_clean = cleaner.clean_sales(sales)
    stores_clean = cleaner.clean_stores(stores)
    holidays_clean = cleaner.clean_holidays(holidays)
    oil_clean = cleaner.clean_oil(oil)

    # Merge all into one dataset
    merged = cleaner.merge_all(sales_clean, stores_clean, holidays_clean, oil_clean)

    # Save
    os.makedirs('data/cleaned', exist_ok=True)
    merged.to_csv('data/cleaned/merged_retail_data.csv', index=False)

    # Print report
    cleaner.print_report()

    print(f"\n💾 Saved: data/cleaned/merged_retail_data.csv")
    print(f"   Rows: {len(merged):,}")
    print(f"   Columns: {len(merged.columns)}")
    print(f"   Size: {os.path.getsize('data/cleaned/merged_retail_data.csv') / (1024*1024):.1f} MB")
    print(f"\n✅ Part 1 DONE! Now run: python src/feature_engineering.py")