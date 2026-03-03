"""
DAY 4 — PART 1: ETL Pipeline
Extract from SQL Server -> Transform -> Load
"""
import pandas as pd
import numpy as np
import pyodbc
try:
    from sqlalchemy import create_engine
    HAS_SQLALCHEMY = True
except ImportError:
    HAS_SQLALCHEMY = False
import os
import time
import logging
from datetime import datetime

os.makedirs('logs', exist_ok=True)
os.makedirs('data/processed', exist_ok=True)

# FIX: Use UTF-8 encoding for log file to handle all characters
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler('logs/etl_pipeline.log', encoding='utf-8'),
        logging.StreamHandler(open(1, 'w', encoding='utf-8', closefd=False))
    ]
)
logger = logging.getLogger("ETL")


class ETLPipeline:

    def __init__(self, server, database):
        self.server = server
        self.database = database
        self.conn = None
        self.etl_stats = {
            'start_time': None,
            'end_time': None,
            'rows_extracted': 0,
            'rows_transformed': 0,
            'rows_loaded': 0,
            'errors': 0
        }

    def connect(self):
        logger.info(f"Connecting to {self.server}/{self.database}...")
        try:
            self.conn = pyodbc.connect(
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"Trusted_Connection=yes;"
            )
            logger.info("   Connected successfully!")
            return True
        except Exception as e:
            logger.error(f"   Connection failed: {e}")
            return False

    def extract(self):
        logger.info("EXTRACT: Reading from warehouse...")
        self.etl_stats['start_time'] = datetime.now()

        query = """
        SELECT 
            d.full_date AS date,
            d.year, d.quarter, d.month, d.month_name,
            d.week_of_year, d.day_of_month, d.day_of_week, d.day_name,
            d.day_of_year,
            d.is_weekend, d.is_month_start, d.is_month_end,
            d.is_ramadan_period, d.is_eid_preparation, d.is_summer_peak,
            d.is_wedding_season, d.is_school_season, d.is_payday_week,
            d.is_friday, d.is_end_of_month,
            d.is_holiday, d.is_national_holiday,
            s.store_id, s.city, s.state, s.store_type, s.store_size, s.cluster,
            p.family, p.tajir_category, p.tajir_subcategory,
            p.is_fmcg, p.is_perishable,
            f.sales, f.onpromotion, f.onpromotion_count,
            f.sales_lag_7d, f.sales_lag_14d, f.sales_lag_28d,
            f.sales_rolling_mean_7d, f.sales_rolling_mean_14d, f.sales_rolling_mean_30d,
            f.sales_rolling_std_7d, f.sales_rolling_std_14d,
            f.sales_trend_7d,
            f.is_zero_sale, f.zero_sales_last_7d, f.consecutive_zeros,
            f.store_avg_daily_sales, f.family_avg_sales, f.store_product_avg,
            f.oil_price
        FROM fact_daily_sales f
        JOIN dim_date d ON f.date_key = d.date_key
        JOIN dim_store s ON f.store_key = s.store_key
        JOIN dim_product p ON f.product_key = p.product_key
        ORDER BY d.full_date, s.store_id, p.family
        """

        start = time.time()
        if HAS_SQLALCHEMY:
            engine = create_engine(
                f"mssql+pyodbc://@{self.server}/{self.database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
            )
            df = pd.read_sql(query, engine)
        else:
            df = pd.read_sql(query, self.conn)
        elapsed = time.time() - start

        self.etl_stats['rows_extracted'] = len(df)
        logger.info(f"   Extracted: {len(df):,} rows, {len(df.columns)} columns")
        logger.info(f"   Time: {elapsed:.1f} seconds")
        return df

    def transform(self, df):
        logger.info("TRANSFORM: Validating and enriching data...")

        # Data quality checks
        critical_cols = ['date', 'store_id', 'family', 'sales']
        for col in critical_cols:
            nulls = df[col].isnull().sum()
            if nulls > 0:
                logger.warning(f"   {col} has {nulls} nulls - filling")
                if df[col].dtype in ['float64', 'int64']:
                    df[col] = df[col].fillna(0)
                else:
                    df[col] = df[col].fillna('Unknown')
            else:
                logger.info(f"   {col}: no nulls")

        # Clip negatives
        neg_sales = (df['sales'] < 0).sum()
        if neg_sales > 0:
            df['sales'] = df['sales'].clip(lower=0)
            logger.warning(f"   Clipped {neg_sales} negative sales")
        else:
            logger.info(f"   No negative sales")

        logger.info(f"   Date range: {df['date'].min()} to {df['date'].max()}")

        # Encode categoricals
        logger.info("   Encoding categorical variables...")

        store_type_map = {'A': 5, 'B': 4, 'C': 1, 'D': 3, 'E': 2}
        df['store_type_encoded'] = df['store_type'].map(store_type_map).fillna(0).astype(int)

        family_rank = df.groupby('family')['sales'].sum().rank(ascending=False)
        df['family_encoded'] = df['family'].map(family_rank).fillna(0).astype(int)

        category_map = {'FMCG': 3, 'Fresh': 2, 'Non-Food': 1}
        df['category_encoded'] = df['tajir_category'].map(category_map).fillna(0).astype(int)

        # Interaction features
        logger.info("   Creating interaction features...")
        df['promo_weekend'] = df['onpromotion'] * df['is_weekend']
        df['ramadan_fmcg'] = df['is_ramadan_period'] * df['is_fmcg']
        df['wedding_grocery'] = df['is_wedding_season'] * (df['family'] == 'GROCERY I').astype(int)
        df['payday_spending'] = df['is_payday_week'] * df['category_encoded']

        # Fill remaining NaN
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        df[numeric_cols] = df[numeric_cols].fillna(0)

        self.etl_stats['rows_transformed'] = len(df)
        logger.info(f"   Transformed: {len(df):,} rows, {len(df.columns)} columns")
        return df

    def load(self, df):
        logger.info("LOAD: Saving ML-ready dataset...")

        output_path = 'data/processed/ml_ready_data.csv'
        df.to_csv(output_path, index=False)
        file_size = os.path.getsize(output_path) / (1024 * 1024)
        logger.info(f"   Saved: {output_path} ({file_size:.1f} MB)")

        # Update warehouse metrics
        logger.info("   Updating warehouse metrics...")
        try:
            cursor = self.conn.cursor()

            store_metrics = df.groupby('store_id').agg(
                avg_daily_sales=('sales', 'mean'),
                total_revenue=('sales', 'sum'),
                stockout_rate=('is_zero_sale', 'mean')
            ).reset_index()

            for _, row in store_metrics.iterrows():
                cursor.execute("""
                    UPDATE dim_store 
                    SET avg_daily_sales = ?, total_revenue = ?, stockout_rate = ?
                    WHERE store_id = ?
                """,
                    round(float(row['avg_daily_sales']), 2),
                    round(float(row['total_revenue']), 2),
                    round(float(row['stockout_rate']) * 100, 2),
                    int(row['store_id'])
                )
            self.conn.commit()
            logger.info(f"   Updated {len(store_metrics)} store metrics")

            product_metrics = df.groupby('family').agg(
                avg_daily_sales=('sales', 'mean'),
                total_revenue=('sales', 'sum'),
                stockout_rate=('is_zero_sale', 'mean')
            ).reset_index()
            product_metrics['rank'] = product_metrics['total_revenue'].rank(ascending=False).astype(int)

            for _, row in product_metrics.iterrows():
                cursor.execute("""
                    UPDATE dim_product 
                    SET avg_daily_sales = ?, total_revenue = ?, 
                        stockout_rate = ?, popularity_rank = ?
                    WHERE family = ?
                """,
                    round(float(row['avg_daily_sales']), 2),
                    round(float(row['total_revenue']), 2),
                    round(float(row['stockout_rate']) * 100, 2),
                    int(row['rank']),
                    str(row['family'])
                )
            self.conn.commit()
            logger.info(f"   Updated {len(product_metrics)} product metrics")
        except Exception as e:
            logger.warning(f"   Metric update skipped: {e}")

        self.etl_stats['rows_loaded'] = len(df)
        self.etl_stats['end_time'] = datetime.now()
        return df

    def run(self):
        print("=" * 60)
        print("   ETL PIPELINE - Extract > Transform > Load")
        print("=" * 60)

        if not self.connect():
            return None

        df = self.extract()
        df = self.transform(df)
        df = self.load(df)

        elapsed = (self.etl_stats['end_time'] - self.etl_stats['start_time']).total_seconds()

        print(f"\n{'=' * 60}")
        print(f"   ETL PIPELINE REPORT")
        print(f"{'=' * 60}")
        print(f"   Start:       {self.etl_stats['start_time']}")
        print(f"   End:         {self.etl_stats['end_time']}")
        print(f"   Duration:    {elapsed:.1f} seconds ({elapsed/60:.1f} min)")
        print(f"   Extracted:   {self.etl_stats['rows_extracted']:,} rows")
        print(f"   Transformed: {self.etl_stats['rows_transformed']:,} rows")
        print(f"   Loaded:      {self.etl_stats['rows_loaded']:,} rows")
        print(f"   Errors:      {self.etl_stats['errors']}")
        print(f"{'=' * 60}")

        self.conn.close()
        return df


if __name__ == "__main__":
    SERVER_NAME = "localhost\\SQLEXPRESS"
    DATABASE = "TajirRetailDW"

    pipeline = ETLPipeline(SERVER_NAME, DATABASE)
    result = pipeline.run()

    if result is not None:
        print(f"\n   ETL COMPLETE! ML-ready data: data/processed/ml_ready_data.csv")
        print(f"   Rows: {len(result):,}")
        print(f"   Columns: {len(result.columns)}")
        print(f"\n   Now run: python src/train_model.py")
    else:
        print(f"\n   ETL failed - check connection settings")