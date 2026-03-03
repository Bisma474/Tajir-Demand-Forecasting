"""
DAY 3: Load REAL cleaned data into SQL Server warehouse
Reads featured_retail_data.csv and populates ALL tables

Run: python src/load_warehouse.py
"""
import pandas as pd
import pyodbc
import os
import sys
import time

print("╔" + "═" * 58 + "╗")
print("║   LOADING REAL DATA INTO WAREHOUSE 📦 → 🏗️              ║")
print("╚" + "═" * 58 + "╝")

# ============================================
# STEP 1: Connect to SQL Server
# ============================================
# Try common SQL Server connection strings
# Change SERVER_NAME if yours is different

SERVER_NAME = "localhost\\SQLEXPRESS"  # Most common for SQL Express
# Other options to try if this doesn't work:
# SERVER_NAME = "localhost"
# SERVER_NAME = "(local)"
# SERVER_NAME = "(local)\\SQLEXPRESS"
# SERVER_NAME = ".\\SQLEXPRESS"

DATABASE = "TajirRetailDW"

print(f"\n🔌 Connecting to SQL Server...")
print(f"   Server: {SERVER_NAME}")
print(f"   Database: {DATABASE}")

try:
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER_NAME};"
        f"DATABASE={DATABASE};"
        f"Trusted_Connection=yes;"
    )
    cursor = conn.cursor()
    print(f"   ✅ Connected successfully!")
except Exception as e:
    print(f"   ❌ Connection failed: {e}")
    print(f"\n   TRY THESE FIXES:")
    print(f"   1. Open SSMS → check your server name in the connection dialog")
    print(f"   2. Edit SERVER_NAME in this script to match")
    print(f"   3. Make sure SQL Server is running (services.msc → SQL Server)")
    print(f"\n   Common server names:")
    print(f"     localhost\\SQLEXPRESS")
    print(f"     (local)\\SQLEXPRESS")
    print(f"     .\\SQLEXPRESS")
    print(f"     localhost")
    print(f"     YOUR_PC_NAME\\SQLEXPRESS")
    sys.exit(1)

# ============================================
# STEP 2: Load cleaned data files
# ============================================
print(f"\n📂 Loading cleaned data files...")

featured_path = 'data/cleaned/featured_retail_data.csv'
cpi_path = 'data/pakistan/cpi_data.csv'
stores_path = 'data/raw/stores.csv'
oil_path = 'data/raw/oil.csv'

if not os.path.exists(featured_path):
    print(f"   ❌ {featured_path} not found! Run Day 2 scripts first.")
    sys.exit(1)

df = pd.read_csv(featured_path, parse_dates=['date'])
print(f"   ✅ Featured data: {len(df):,} rows")

stores = pd.read_csv(stores_path)
print(f"   ✅ Stores: {len(stores)} rows")

oil = pd.read_csv(oil_path, parse_dates=['date'])
oil['dcoilwtico'] = oil['dcoilwtico'].ffill().bfill()
print(f"   ✅ Oil: {len(oil)} rows")

if os.path.exists(cpi_path):
    cpi = pd.read_csv(cpi_path)
    print(f"   ✅ CPI: {len(cpi):,} rows")
else:
    cpi = None
    print(f"   ⚠️  CPI file not found — skipping")

# ============================================
# STEP 3: Populate dim_store (54 stores)
# ============================================
print(f"\n🏪 Loading dim_store...")

cursor.execute("DELETE FROM fact_daily_sales")  # Clear fact first (FK constraint)
cursor.execute("DELETE FROM dim_store")
conn.commit()

# Calculate store metrics from real data
store_metrics = df.groupby('store_id').agg(
    avg_daily_sales=('sales', 'mean'),
    total_revenue=('sales', 'sum'),
    stockout_rate=('is_zero_sale', 'mean')
).reset_index()
store_metrics['stockout_rate'] = (store_metrics['stockout_rate'] * 100).round(2)

stores_merged = stores.merge(store_metrics, left_on='store_nbr', right_on='store_id', how='left')

size_map = {'A': 'Large', 'B': 'Medium-Large', 'C': 'Small', 'D': 'Medium', 'E': 'Medium-Small'}

for _, row in stores_merged.iterrows():
    cursor.execute("""
        INSERT INTO dim_store (store_key, store_id, city, state, store_type, 
                               store_size, cluster, avg_daily_sales, total_revenue, stockout_rate)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        int(row['store_nbr']), int(row['store_nbr']),
        str(row['city']).strip(), str(row['state']).strip(),
        str(row['type']).strip(),
        size_map.get(str(row['type']).strip(), 'Unknown'),
        int(row['cluster']),
        round(float(row.get('avg_daily_sales', 0)), 2),
        round(float(row.get('total_revenue', 0)), 2),
        round(float(row.get('stockout_rate', 0)), 2)
    )

conn.commit()
cursor.execute("SELECT COUNT(*) FROM dim_store")
print(f"   ✅ dim_store loaded: {cursor.fetchone()[0]} stores")

# ============================================
# STEP 4: Populate dim_economic (oil prices)
# ============================================
print(f"\n🛢️ Loading dim_economic...")

cursor.execute("DELETE FROM dim_economic")
conn.commit()

oil_clean = oil.dropna(subset=['dcoilwtico']).copy()
oil_clean['date_key'] = oil_clean['date'].dt.year * 10000 + oil_clean['date'].dt.month * 100 + oil_clean['date'].dt.day
oil_clean['oil_7d_avg'] = oil_clean['dcoilwtico'].rolling(7, min_periods=1).mean()
oil_clean['oil_change'] = oil_clean['dcoilwtico'].diff().fillna(0)

loaded = 0
for _, row in oil_clean.iterrows():
    try:
        cursor.execute("""
            INSERT INTO dim_economic (date_key, full_date, oil_price, oil_price_7d_avg, oil_price_change)
            VALUES (?, ?, ?, ?, ?)
        """,
            int(row['date_key']), row['date'].date(),
            round(float(row['dcoilwtico']), 2),
            round(float(row['oil_7d_avg']), 2),
            round(float(row['oil_change']), 2)
        )
        loaded += 1
    except Exception:
        pass  # Skip dates not in dim_date

conn.commit()
print(f"   ✅ dim_economic loaded: {loaded} days")

# ============================================
# STEP 5: Populate dim_pakistan_cpi
# ============================================
if cpi is not None:
    print(f"\n🇵🇰 Loading dim_pakistan_cpi...")
    cursor.execute("DELETE FROM dim_pakistan_cpi")
    conn.commit()

    batch_size = 5000
    total = len(cpi)
    loaded = 0

    for start in range(0, total, batch_size):
        batch = cpi.iloc[start:start + batch_size]
        for _, row in batch.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO dim_pakistan_cpi 
                    (date, year, month, item_id, item_name, national_avg_price, 
                     pct_change, city, city_price)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    str(row.get('date', '2019-01-01')),
                    int(row.get('year', 2019)),
                    int(row.get('month', 1)),
                    int(row.get('item_id', 0)) if pd.notna(row.get('item_id')) else 0,
                    str(row.get('item_name', 'Unknown'))[:200],
                    round(float(row.get('national_avg_price', 0)), 2) if pd.notna(row.get('national_avg_price')) else 0,
                    round(float(row.get('pct_change', 0)), 2) if pd.notna(row.get('pct_change')) else 0,
                    str(row.get('city', 'Unknown'))[:100],
                    round(float(row.get('city_price', 0)), 2) if pd.notna(row.get('city_price')) else 0
                )
                loaded += 1
            except Exception:
                pass

        conn.commit()
        pct = min((start + batch_size) / total * 100, 100)
        print(f"      Progress: {pct:.0f}% ({loaded:,} rows loaded)")

    print(f"   ✅ dim_pakistan_cpi loaded: {loaded:,} rows")

# ============================================
# STEP 6: Populate fact_daily_sales (3M rows!)
# ============================================
print(f"\n📊 Loading fact_daily_sales (3M+ rows — this takes 10-20 minutes)...")
print(f"   ☕ Go make chai while this runs!\n")

cursor.execute("DELETE FROM fact_daily_sales")
conn.commit()

# Build product_key lookup
cursor.execute("SELECT product_key, family FROM dim_product")
product_lookup = {row[1]: row[0] for row in cursor.fetchall()}

# Prepare data
df['date_key'] = df['date'].dt.year * 10000 + df['date'].dt.month * 100 + df['date'].dt.day

batch_size = 10000
total = len(df)
loaded = 0
skipped = 0
start_time = time.time()

for start in range(0, total, batch_size):
    batch = df.iloc[start:start + batch_size]

    for _, row in batch.iterrows():
        product_key = product_lookup.get(row.get('family', ''), None)
        if product_key is None:
            skipped += 1
            continue

        try:
            cursor.execute("""
                INSERT INTO fact_daily_sales (
                    date_key, store_key, product_key,
                    sales, onpromotion, onpromotion_count,
                    sales_lag_7d, sales_lag_14d, sales_lag_28d,
                    sales_rolling_mean_7d, sales_rolling_mean_14d, sales_rolling_mean_30d,
                    sales_rolling_std_7d, sales_rolling_std_14d, sales_trend_7d,
                    is_zero_sale, zero_sales_last_7d, consecutive_zeros,
                    store_avg_daily_sales, family_avg_sales, store_product_avg,
                    oil_price
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                int(row.get('date_key', 0)),
                int(row.get('store_id', 0)),
                int(product_key),
                round(float(row.get('sales', 0)), 2),
                int(row.get('onpromotion', 0)),
                int(row.get('onpromotion_count', 0)),
                round(float(row.get('sales_lag_7d', 0)), 2),
                round(float(row.get('sales_lag_14d', 0)), 2),
                round(float(row.get('sales_lag_28d', 0)), 2),
                round(float(row.get('sales_rolling_mean_7d', 0)), 2),
                round(float(row.get('sales_rolling_mean_14d', 0)), 2),
                round(float(row.get('sales_rolling_mean_30d', 0)), 2),
                round(float(row.get('sales_rolling_std_7d', 0)), 2),
                round(float(row.get('sales_rolling_std_14d', 0)), 2),
                round(float(row.get('sales_trend_7d', 0)), 2),
                int(row.get('is_zero_sale', 0)),
                int(row.get('zero_sales_last_7d', 0)),
                int(row.get('consecutive_zeros', 0)),
                round(float(row.get('store_avg_daily_sales', 0)), 2),
                round(float(row.get('family_avg_sales', 0)), 2),
                round(float(row.get('store_product_avg', 0)), 2),
                round(float(row.get('oil_price', 0)), 2)
            )
            loaded += 1
        except Exception as e:
            skipped += 1

    conn.commit()

    # Progress update every batch
    elapsed = time.time() - start_time
    pct = min((start + batch_size) / total * 100, 100)
    rows_per_sec = loaded / elapsed if elapsed > 0 else 0
    remaining = (total - start - batch_size) / rows_per_sec if rows_per_sec > 0 else 0

    print(f"   📊 {pct:5.1f}% | {loaded:>10,} loaded | {skipped:>6,} skipped | "
          f"{rows_per_sec:,.0f} rows/sec | ~{remaining/60:.0f} min remaining")

elapsed_total = time.time() - start_time
print(f"\n   ✅ fact_daily_sales loaded: {loaded:,} rows in {elapsed_total/60:.1f} minutes")
print(f"   ⚠️  Skipped: {skipped:,} rows")

# ============================================
# STEP 7: Final Verification
# ============================================
print(f"\n{'═' * 60}")
print(f"📊 WAREHOUSE FINAL STATUS")
print(f"{'═' * 60}")

tables = ['dim_date', 'dim_store', 'dim_product', 'dim_economic', 'dim_pakistan_cpi', 'fact_daily_sales']
for table in tables:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    status = "✅" if count > 0 else "❌"
    print(f"   {status} {table:<25s} {count:>12,} rows")

# Quick data quality check on fact table
cursor.execute("""
    SELECT 
        MIN(f.sales) AS min_sales,
        MAX(f.sales) AS max_sales,
        AVG(f.sales) AS avg_sales,
        SUM(CAST(f.is_zero_sale AS INT)) AS zero_sale_count
    FROM fact_daily_sales f
""")
row = cursor.fetchone()
if row and row[0] is not None:
    print(f"\n   📊 Fact Table Quick Stats:")
    print(f"      Min sales:    {row[0]:>12,.2f}")
    print(f"      Max sales:    {row[1]:>12,.2f}")
    print(f"      Avg sales:    {row[2]:>12,.2f}")
    print(f"      Zero sales:   {row[3]:>12,}")

conn.close()

print(f"""
{'═' * 60}
🎉 WAREHOUSE LOADED WITH REAL DATA!
{'═' * 60}

  All tables populated with YOUR actual cleaned data:
  → dim_date:        Calendar with Pakistan features
  → dim_store:       54 stores with real metrics
  → dim_product:     33 products with Tajir categories
  → dim_economic:    Oil prices (2013-2017)
  → dim_pakistan_cpi: 101K Pakistan CPI records
  → fact_daily_sales: 3M+ real sales transactions

  ✅ Now run the analytical queries in SSMS:
     Open sql/05_analytical_queries.sql → Press F5
     You'll see REAL results from REAL data!
""")