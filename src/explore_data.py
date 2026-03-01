"""
DAY 1: Complete Data Exploration
Understand all datasets before cleaning them
"""
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend (no display needed)
import matplotlib.pyplot as plt
import seaborn as sns
import os
import warnings
warnings.filterwarnings('ignore')

os.makedirs('screenshots', exist_ok=True)

print("╔" + "═" * 58 + "╗")
print("║         DAY 1: DATA EXPLORATION — LET'S GO! 🔍          ║")
print("╚" + "═" * 58 + "╝")


# ============================================
# PART 1: LOAD ALL DATASETS
# ============================================
print("\n📂 LOADING ALL DATASETS...")
print("─" * 60)

train = pd.read_csv('data/raw/train.csv', parse_dates=['date'])
stores = pd.read_csv('data/raw/stores.csv')
oil = pd.read_csv('data/raw/oil.csv', parse_dates=['date'])
holidays = pd.read_csv('data/raw/holidays_events.csv', parse_dates=['date'])
cpi = pd.read_csv('data/pakistan/cpi_data.csv')

print(f"  📊 train.csv:            {train.shape[0]:>10,} rows × {train.shape[1]} cols")
print(f"  📊 stores.csv:           {stores.shape[0]:>10,} rows × {stores.shape[1]} cols")
print(f"  📊 oil.csv:              {oil.shape[0]:>10,} rows × {oil.shape[1]} cols")
print(f"  📊 holidays_events.csv:  {holidays.shape[0]:>10,} rows × {holidays.shape[1]} cols")
print(f"  📊 cpi_data.csv:         {cpi.shape[0]:>10,} rows × {cpi.shape[1]} cols")
print(f"\n  💾 Total data loaded: {train.shape[0] + stores.shape[0] + oil.shape[0] + holidays.shape[0] + cpi.shape[0]:,} rows")


# ============================================
# PART 2: DEEP DIVE — TRAIN DATA (Main Dataset)
# ============================================
print(f"\n{'═' * 60}")
print(f"📋 TRAIN DATA — Sales Transactions (MAIN DATASET)")
print(f"{'═' * 60}")

print(f"\n  📅 Date Range: {train['date'].min().date()} to {train['date'].max().date()}")
print(f"  📅 Total Days: {train['date'].nunique()}")
print(f"  🏪 Unique Stores: {train['store_nbr'].nunique()}")
print(f"  📦 Product Families: {train['family'].nunique()}")
print(f"  📈 Total Records: {len(train):,}")

print(f"\n  🔍 Data Types:")
for col in train.columns:
    print(f"     {col:<20s} {str(train[col].dtype):<15s} Nulls: {train[col].isnull().sum()}")

print(f"\n  📊 Sales Statistics:")
print(f"     Mean:   {train['sales'].mean():>12,.2f}")
print(f"     Median: {train['sales'].median():>12,.2f}")
print(f"     Min:    {train['sales'].min():>12,.2f}")
print(f"     Max:    {train['sales'].max():>12,.2f}")
print(f"     Std:    {train['sales'].std():>12,.2f}")

# Zero sales analysis — THIS IS KEY FOR STOCKOUT DETECTION
zero_sales = (train['sales'] == 0).sum()
zero_pct = zero_sales / len(train) * 100
print(f"\n  ⚠️  ZERO SALES ANALYSIS (Potential Stockouts!):")
print(f"     Zero sales rows: {zero_sales:,} ({zero_pct:.1f}% of all data)")
print(f"     Non-zero rows:   {len(train) - zero_sales:,}")
print(f"     → This means ~{zero_pct:.0f}% of the time, stores had NO sales")
print(f"     → These could be STOCKOUT events — key for our project!")

# Negative sales
neg_sales = (train['sales'] < 0).sum()
print(f"\n  📉 Negative sales: {neg_sales} rows")

# Product families breakdown
print(f"\n  📦 ALL {train['family'].nunique()} PRODUCT FAMILIES:")
print(f"  {'─' * 55}")
family_stats = train.groupby('family').agg(
    total_sales=('sales', 'sum'),
    avg_daily=('sales', 'mean'),
    zero_days=('sales', lambda x: (x == 0).sum())
).sort_values('total_sales', ascending=False)

for i, (family, row) in enumerate(family_stats.iterrows(), 1):
    bar = '█' * min(int(row['total_sales'] / family_stats['total_sales'].max() * 20), 20)
    print(f"  {i:2d}. {family:<32s} {bar} {row['total_sales']:>12,.0f}")

# Promotion analysis
if 'onpromotion' in train.columns:
    promo_count = train['onpromotion'].sum()
    promo_pct = promo_count / len(train) * 100
    print(f"\n  📢 PROMOTIONS:")
    print(f"     Rows with promotion: {promo_count:,} ({promo_pct:.1f}%)")
    
    promo_avg = train[train['onpromotion'] > 0]['sales'].mean()
    normal_avg = train[train['onpromotion'] == 0]['sales'].mean()
    lift = ((promo_avg - normal_avg) / normal_avg) * 100 if normal_avg > 0 else 0
    print(f"     Avg sales WITH promotion:    {promo_avg:>10,.2f}")
    print(f"     Avg sales WITHOUT promotion: {normal_avg:>10,.2f}")
    print(f"     Promotion lift: {lift:+.1f}%")


# ============================================
# PART 3: STORES DATA
# ============================================
print(f"\n{'═' * 60}")
print(f"🏪 STORES DATA — Store Metadata")
print(f"{'═' * 60}")

print(f"\n  Total stores: {len(stores)}")
print(f"\n  Store Types:")
for stype, count in stores['type'].value_counts().items():
    print(f"     Type {stype}: {count} stores")

print(f"\n  Cities ({stores['city'].nunique()} unique):")
for city, count in stores['city'].value_counts().items():
    print(f"     {city}: {count} stores")

print(f"\n  States ({stores['state'].nunique()} unique):")
for state, count in stores['state'].value_counts().items():
    print(f"     {state}: {count} stores")

print(f"\n  Clusters: {sorted(stores['cluster'].unique().tolist())}")


# ============================================
# PART 4: HOLIDAYS DATA
# ============================================
print(f"\n{'═' * 60}")
print(f"🎉 HOLIDAYS & EVENTS DATA")
print(f"{'═' * 60}")

print(f"\n  Total events: {len(holidays)}")
print(f"  Date range: {holidays['date'].min().date()} to {holidays['date'].max().date()}")

print(f"\n  Event Types:")
for etype, count in holidays['type'].value_counts().items():
    print(f"     {etype}: {count}")

print(f"\n  Transferred holidays: {holidays['transferred'].sum()}")

print(f"\n  Sample events:")
for _, row in holidays.head(10).iterrows():
    print(f"     {row['date'].date()} | {row['type']:<10s} | {row['description']}")


# ============================================
# PART 5: OIL DATA
# ============================================
print(f"\n{'═' * 60}")
print(f"🛢️ OIL PRICE DATA")
print(f"{'═' * 60}")

print(f"\n  Date range: {oil['date'].min().date()} to {oil['date'].max().date()}")
print(f"  Total days: {len(oil)}")
print(f"  Null prices: {oil['dcoilwtico'].isnull().sum()} ({oil['dcoilwtico'].isnull().mean()*100:.1f}%)")
print(f"  Price range: ${oil['dcoilwtico'].min():.2f} to ${oil['dcoilwtico'].max():.2f}")
print(f"  Average: ${oil['dcoilwtico'].mean():.2f}")


# ============================================
# PART 6: PAKISTAN CPI DATA
# ============================================
print(f"\n{'═' * 60}")
print(f"🇵🇰 PAKISTAN CPI DATA (Consumer Price Index)")
print(f"{'═' * 60}")

print(f"\n  Total records: {len(cpi):,}")
print(f"  Years: {sorted(cpi['year'].unique().tolist())}")
print(f"  Cities: {cpi['city'].nunique()}")
print(f"  Items tracked: {cpi['item_name'].nunique()}")
print(f"  Columns: {cpi.columns.tolist()}")

print(f"\n  🏙️ Cities:")
for city in sorted(cpi['city'].unique()):
    print(f"     → {city}")

print(f"\n  📦 Sample items:")
for item in cpi['item_name'].unique()[:20]:
    print(f"     → {item}")

# Lahore specific
lahore_cpi = cpi[cpi['city'] == 'Lahore']
if len(lahore_cpi) > 0:
    print(f"\n  📍 LAHORE DATA:")
    print(f"     Records: {len(lahore_cpi):,}")
    print(f"     Avg price (all items): PKR {lahore_cpi['city_price'].mean():,.1f}")


# ============================================
# PART 7: GENERATE EXPLORATION CHARTS
# ============================================
print(f"\n{'═' * 60}")
print(f"📊 GENERATING EXPLORATION CHARTS...")
print(f"{'═' * 60}")

fig, axes = plt.subplots(3, 2, figsize=(18, 16))
fig.suptitle('Day 1: Data Exploration — Tajir Demand Forecasting', fontsize=16, fontweight='bold')

# Chart 1: Daily total sales over time
daily_sales = train.groupby('date')['sales'].sum()
axes[0, 0].plot(daily_sales.index, daily_sales.values, linewidth=0.5, color='#2E86C1')
axes[0, 0].set_title('📈 Daily Total Sales Over Time', fontsize=12)
axes[0, 0].set_xlabel('Date')
axes[0, 0].set_ylabel('Total Sales')
axes[0, 0].grid(True, alpha=0.3)

# Chart 2: Top 15 product families
top_families = train.groupby('family')['sales'].sum().sort_values(ascending=True).tail(15)
top_families.plot(kind='barh', ax=axes[0, 1], color='#27AE60')
axes[0, 1].set_title('🏆 Top 15 Product Families by Sales', fontsize=12)
axes[0, 1].set_xlabel('Total Sales')

# Chart 3: Monthly seasonal pattern
train['month'] = train['date'].dt.month
monthly = train.groupby('month')['sales'].mean()
month_names = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
axes[1, 0].bar(month_names, monthly.values, color='#E74C3C')
axes[1, 0].set_title('📅 Average Sales by Month (Seasonal Pattern)', fontsize=12)
axes[1, 0].set_ylabel('Avg Sales')

# Chart 4: Day of week pattern
train['dow'] = train['date'].dt.dayofweek
dow_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
dow_sales = train.groupby('dow')['sales'].mean()
colors = ['#3498DB'] * 5 + ['#E67E22'] * 2  # weekdays blue, weekends orange
axes[1, 1].bar(dow_names, dow_sales.values, color=colors)
axes[1, 1].set_title('📅 Average Sales by Day of Week', fontsize=12)
axes[1, 1].set_ylabel('Avg Sales')

# Chart 5: Oil price over time
oil_clean = oil.dropna(subset=['dcoilwtico'])
axes[2, 0].plot(oil_clean['date'], oil_clean['dcoilwtico'], linewidth=0.8, color='#8E44AD')
axes[2, 0].set_title('🛢️ Oil Price Over Time', fontsize=12)
axes[2, 0].set_ylabel('USD per Barrel')
axes[2, 0].grid(True, alpha=0.3)

# Chart 6: Pakistan CPI — Lahore prices over time (if available)
if len(lahore_cpi) > 0:
    # Get a key item — wheat flour
    wheat = lahore_cpi[lahore_cpi['item_name'].str.contains('Wheat', case=False, na=False)]
    if len(wheat) > 0:
        wheat_monthly = wheat.groupby(['year', 'month'])['city_price'].mean().reset_index()
        wheat_monthly['date_str'] = wheat_monthly['year'].astype(str) + '-' + wheat_monthly['month'].astype(str).str.zfill(2)
        axes[2, 1].plot(range(len(wheat_monthly)), wheat_monthly['city_price'].values, 
                        linewidth=2, color='#E74C3C', marker='o', markersize=3)
        axes[2, 1].set_title('🇵🇰 Wheat Flour Price in Lahore (2019-2024)', fontsize=12)
        axes[2, 1].set_ylabel('PKR')
        # Show a few x labels
        step = max(len(wheat_monthly) // 6, 1)
        axes[2, 1].set_xticks(range(0, len(wheat_monthly), step))
        axes[2, 1].set_xticklabels(wheat_monthly['date_str'].iloc[::step], rotation=45)
        axes[2, 1].grid(True, alpha=0.3)
    else:
        axes[2, 1].text(0.5, 0.5, 'CPI item data not available', ha='center', va='center')
        axes[2, 1].set_title('🇵🇰 Pakistan CPI', fontsize=12)
else:
    axes[2, 1].text(0.5, 0.5, 'Lahore CPI data not available', ha='center', va='center')
    axes[2, 1].set_title('🇵🇰 Pakistan CPI', fontsize=12)

plt.tight_layout()
plt.savefig('screenshots/day1_exploration.png', dpi=150, bbox_inches='tight')
print(f"\n  ✅ Chart saved: screenshots/day1_exploration.png")


# ============================================
# PART 8: CHART 2 — Deeper Analysis
# ============================================
fig2, axes2 = plt.subplots(2, 2, figsize=(16, 12))
fig2.suptitle('Day 1: Deeper Analysis — Stockout & Seasonal Patterns', fontsize=16, fontweight='bold')

# Chart 7: Zero sales distribution by product (stockout indicator)
zero_by_family = train[train['sales'] == 0].groupby('family').size().sort_values(ascending=True).tail(15)
zero_by_family.plot(kind='barh', ax=axes2[0, 0], color='#E74C3C')
axes2[0, 0].set_title('⚠️ Products with Most Zero-Sales Days (Stockout Risk)', fontsize=11)
axes2[0, 0].set_xlabel('Number of Zero-Sales Days')

# Chart 8: Sales by store type
store_sales = train.merge(stores[['store_nbr', 'type']], on='store_nbr')
type_sales = store_sales.groupby('type')['sales'].mean().sort_values()
type_sales.plot(kind='bar', ax=axes2[0, 1], color='#3498DB')
axes2[0, 1].set_title('🏪 Average Sales by Store Type', fontsize=11)
axes2[0, 1].set_ylabel('Avg Daily Sales')

# Chart 9: Promotion vs No Promotion
if 'onpromotion' in train.columns:
    promo_comparison = train.groupby(train['onpromotion'] > 0)['sales'].mean()
    labels = ['No Promotion', 'With Promotion']
    colors = ['#95A5A6', '#27AE60']
    axes2[1, 0].bar(labels, promo_comparison.values, color=colors)
    axes2[1, 0].set_title('📢 Promotion Impact on Sales', fontsize=11)
    axes2[1, 0].set_ylabel('Avg Sales')

# Chart 10: Year-over-year trend
train['year'] = train['date'].dt.year
yearly = train.groupby('year')['sales'].sum()
yearly.plot(kind='bar', ax=axes2[1, 1], color='#F39C12')
axes2[1, 1].set_title('📅 Total Sales by Year', fontsize=11)
axes2[1, 1].set_ylabel('Total Sales')

plt.tight_layout()
plt.savefig('screenshots/day1_deeper_analysis.png', dpi=150, bbox_inches='tight')
print(f"  ✅ Chart saved: screenshots/day1_deeper_analysis.png")


# ============================================
# PART 9: KEY FINDINGS SUMMARY
# ============================================
print(f"\n{'═' * 60}")
print(f"📝 KEY FINDINGS — DAY 1 EXPLORATION")
print(f"{'═' * 60}")

findings = f"""
  1. DATASET SIZE: {len(train):,} sales records across {train['date'].nunique()} days
     → {train['store_nbr'].nunique()} stores, {train['family'].nunique()} product categories

  2. STOCKOUT SIGNAL: {zero_pct:.1f}% of all records have ZERO sales
     → This is our MAIN business problem to solve!
     → {zero_sales:,} potential stockout events

  3. TOP PRODUCTS: {family_stats.index[0]} is the highest selling category
     → Bottom sellers may have supply issues

  4. SEASONALITY: Monthly patterns visible (check chart)
     → This validates our Ramadan/Summer/Wedding features

  5. PROMOTIONS: {promo_pct:.1f}% of records have active promotions
     → Promotions boost sales by ~{lift:.0f}%

  6. OIL PRICES: Range ${oil['dcoilwtico'].min():.0f}-${oil['dcoilwtico'].max():.0f}
     → Economic indicator that affects supply chain costs

  7. PAKISTAN CPI: {cpi['item_name'].nunique()} daily-use items tracked
     → Price data from {cpi['city'].nunique()} Pakistani cities
     → Shows real inflation impact on kiryana store costs

  8. NEGATIVE SALES: {neg_sales} rows (returns/errors)
     → Will be cleaned in Day 2

  9. NULL VALUES: train nulls = {train.isnull().sum().sum()}, 
     oil nulls = {oil['dcoilwtico'].isnull().sum()}
     → Will be handled in Day 2 cleaning
"""
print(findings)

print(f"{'═' * 60}")
print(f"✅ DAY 1 EXPLORATION COMPLETE!")
print(f"{'═' * 60}")
print(f"""
  📊 Charts saved to screenshots/ folder
  📝 Key findings documented above
  
  🔜 NEXT: Day 2 — Data Cleaning & Feature Engineering
     → Clean all nulls, duplicates, outliers
     → Merge all sources into one dataset
     → Create 20+ ML features including Pakistan seasonal ones
""")