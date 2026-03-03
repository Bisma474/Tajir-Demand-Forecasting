"""
Creates ALL SQL files for the warehouse automatically
Run this ONCE to generate all 5 SQL scripts
"""
import os

os.makedirs('sql', exist_ok=True)

# ============================================
# FILE 1: Create Database
# ============================================
with open('sql/01_create_database.sql', 'w') as f:
    f.write("""-- ============================================================
-- DAY 3: CREATE TAJIR RETAIL DATA WAREHOUSE
-- ============================================================

IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'TajirRetailDW')
BEGIN
    CREATE DATABASE TajirRetailDW;
END
GO

USE TajirRetailDW;
GO

SELECT 'Database TajirRetailDW ready' AS status;
GO
""")

# ============================================
# FILE 2: Dimension Tables
# ============================================
with open('sql/02_dimension_tables.sql', 'w') as f:
    f.write("""-- ============================================================
-- DIMENSION TABLES - Star Schema
-- ============================================================

USE TajirRetailDW;
GO

-- DIM_DATE
IF OBJECT_ID('dim_date', 'U') IS NOT NULL DROP TABLE dim_date;
CREATE TABLE dim_date (
    date_key            INT PRIMARY KEY,
    full_date           DATE NOT NULL UNIQUE,
    year                SMALLINT NOT NULL,
    quarter             TINYINT NOT NULL,
    month               TINYINT NOT NULL,
    month_name          VARCHAR(20) NOT NULL,
    week_of_year        TINYINT NOT NULL,
    day_of_month        TINYINT NOT NULL,
    day_of_week         TINYINT NOT NULL,
    day_name            VARCHAR(20) NOT NULL,
    day_of_year         SMALLINT NOT NULL,
    is_weekend          BIT NOT NULL DEFAULT 0,
    is_month_start      BIT NOT NULL DEFAULT 0,
    is_month_end        BIT NOT NULL DEFAULT 0,
    is_ramadan_period   BIT NOT NULL DEFAULT 0,
    is_eid_preparation  BIT NOT NULL DEFAULT 0,
    is_summer_peak      BIT NOT NULL DEFAULT 0,
    is_wedding_season   BIT NOT NULL DEFAULT 0,
    is_school_season    BIT NOT NULL DEFAULT 0,
    is_payday_week      BIT NOT NULL DEFAULT 0,
    is_friday           BIT NOT NULL DEFAULT 0,
    is_end_of_month     BIT NOT NULL DEFAULT 0,
    is_holiday          BIT NOT NULL DEFAULT 0,
    is_national_holiday BIT NOT NULL DEFAULT 0
);
SELECT 'dim_date created' AS status;
GO

-- DIM_STORE
IF OBJECT_ID('dim_store', 'U') IS NOT NULL DROP TABLE dim_store;
CREATE TABLE dim_store (
    store_key           INT PRIMARY KEY,
    store_id            INT NOT NULL UNIQUE,
    city                VARCHAR(100) NOT NULL,
    state               VARCHAR(100) NOT NULL,
    store_type          CHAR(1) NOT NULL,
    store_size          VARCHAR(20) NOT NULL,
    cluster             INT NOT NULL,
    avg_daily_sales     DECIMAL(12,2) DEFAULT 0,
    total_revenue       DECIMAL(15,2) DEFAULT 0,
    stockout_rate       DECIMAL(5,2) DEFAULT 0
);
SELECT 'dim_store created' AS status;
GO

-- DIM_PRODUCT
IF OBJECT_ID('dim_product', 'U') IS NOT NULL DROP TABLE dim_product;
CREATE TABLE dim_product (
    product_key         INT IDENTITY(1,1) PRIMARY KEY,
    family              VARCHAR(100) NOT NULL UNIQUE,
    tajir_category      VARCHAR(50),
    tajir_subcategory   VARCHAR(50),
    is_fmcg             BIT NOT NULL DEFAULT 0,
    is_perishable       BIT NOT NULL DEFAULT 0,
    avg_daily_sales     DECIMAL(12,2) DEFAULT 0,
    total_revenue       DECIMAL(15,2) DEFAULT 0,
    popularity_rank     INT DEFAULT 0,
    stockout_rate       DECIMAL(5,2) DEFAULT 0
);
SELECT 'dim_product created' AS status;
GO

-- DIM_ECONOMIC
IF OBJECT_ID('dim_economic', 'U') IS NOT NULL DROP TABLE dim_economic;
CREATE TABLE dim_economic (
    date_key            INT PRIMARY KEY,
    full_date           DATE NOT NULL,
    oil_price           DECIMAL(8,2),
    oil_price_7d_avg    DECIMAL(8,2),
    oil_price_change    DECIMAL(8,2),
    CONSTRAINT FK_economic_date FOREIGN KEY (date_key)
        REFERENCES dim_date(date_key)
);
SELECT 'dim_economic created' AS status;
GO

-- DIM_PAKISTAN_CPI
IF OBJECT_ID('dim_pakistan_cpi', 'U') IS NOT NULL DROP TABLE dim_pakistan_cpi;
CREATE TABLE dim_pakistan_cpi (
    cpi_key             INT IDENTITY(1,1) PRIMARY KEY,
    date                DATE NOT NULL,
    year                SMALLINT NOT NULL,
    month               TINYINT NOT NULL,
    item_id             INT,
    item_name           VARCHAR(200) NOT NULL,
    national_avg_price  DECIMAL(10,2),
    pct_change          DECIMAL(6,2),
    city                VARCHAR(100) NOT NULL,
    city_price          DECIMAL(10,2)
);
CREATE INDEX IX_cpi_city_date ON dim_pakistan_cpi(city, year, month);
SELECT 'dim_pakistan_cpi created' AS status;
GO
""")

# ============================================
# FILE 3: Fact Table
# ============================================
with open('sql/03_fact_tables.sql', 'w') as f:
    f.write("""-- ============================================================
-- FACT TABLE - Center of Star Schema
-- ============================================================

USE TajirRetailDW;
GO

IF OBJECT_ID('fact_daily_sales', 'U') IS NOT NULL DROP TABLE fact_daily_sales;

CREATE TABLE fact_daily_sales (
    sale_key                BIGINT IDENTITY(1,1) PRIMARY KEY,
    date_key                INT NOT NULL,
    store_key               INT NOT NULL,
    product_key             INT NOT NULL,
    sales                   DECIMAL(12,2) NOT NULL DEFAULT 0,
    onpromotion             BIT NOT NULL DEFAULT 0,
    onpromotion_count       INT NOT NULL DEFAULT 0,
    sales_lag_7d            DECIMAL(12,2) DEFAULT 0,
    sales_lag_14d           DECIMAL(12,2) DEFAULT 0,
    sales_lag_28d           DECIMAL(12,2) DEFAULT 0,
    sales_rolling_mean_7d   DECIMAL(12,2) DEFAULT 0,
    sales_rolling_mean_14d  DECIMAL(12,2) DEFAULT 0,
    sales_rolling_mean_30d  DECIMAL(12,2) DEFAULT 0,
    sales_rolling_std_7d    DECIMAL(12,2) DEFAULT 0,
    sales_rolling_std_14d   DECIMAL(12,2) DEFAULT 0,
    sales_trend_7d          DECIMAL(12,2) DEFAULT 0,
    is_zero_sale            BIT NOT NULL DEFAULT 0,
    zero_sales_last_7d      TINYINT DEFAULT 0,
    consecutive_zeros       TINYINT DEFAULT 0,
    store_avg_daily_sales   DECIMAL(12,2) DEFAULT 0,
    family_avg_sales        DECIMAL(12,2) DEFAULT 0,
    store_product_avg       DECIMAL(12,2) DEFAULT 0,
    oil_price               DECIMAL(8,2) DEFAULT 0,
    CONSTRAINT FK_fact_date FOREIGN KEY (date_key)
        REFERENCES dim_date(date_key),
    CONSTRAINT FK_fact_store FOREIGN KEY (store_key)
        REFERENCES dim_store(store_key),
    CONSTRAINT FK_fact_product FOREIGN KEY (product_key)
        REFERENCES dim_product(product_key)
);

CREATE INDEX IX_fact_date ON fact_daily_sales(date_key);
CREATE INDEX IX_fact_store ON fact_daily_sales(store_key);
CREATE INDEX IX_fact_product ON fact_daily_sales(product_key);
CREATE INDEX IX_fact_store_product ON fact_daily_sales(store_key, product_key);
CREATE INDEX IX_fact_date_store ON fact_daily_sales(date_key, store_key);
CREATE INDEX IX_fact_stockout ON fact_daily_sales(is_zero_sale);

SELECT 'fact_daily_sales created with 6 indexes' AS status;
GO
""")

# ============================================
# FILE 4: Populate Dimensions
# ============================================
with open('sql/04_populate_dimensions.sql', 'w') as f:
    f.write("""-- ============================================================
-- POPULATE DIMENSION TABLES
-- ============================================================

USE TajirRetailDW;
GO

-- POPULATE dim_date (2013-01-01 to 2017-12-31)
DELETE FROM dim_date;
GO

;WITH DateRange AS (
    SELECT CAST('2013-01-01' AS DATE) AS dt
    UNION ALL
    SELECT DATEADD(DAY, 1, dt) FROM DateRange WHERE dt < '2017-12-31'
)
INSERT INTO dim_date (
    date_key, full_date, year, quarter, month, month_name,
    week_of_year, day_of_month, day_of_week, day_name, day_of_year,
    is_weekend, is_month_start, is_month_end,
    is_ramadan_period, is_eid_preparation, is_summer_peak,
    is_wedding_season, is_school_season, is_payday_week,
    is_friday, is_end_of_month,
    is_holiday, is_national_holiday
)
SELECT
    YEAR(dt) * 10000 + MONTH(dt) * 100 + DAY(dt),
    dt,
    YEAR(dt),
    DATEPART(QUARTER, dt),
    MONTH(dt),
    DATENAME(MONTH, dt),
    DATEPART(ISO_WEEK, dt),
    DAY(dt),
    (DATEPART(WEEKDAY, dt) + 5) % 7,
    DATENAME(WEEKDAY, dt),
    DATEPART(DAYOFYEAR, dt),
    CASE WHEN DATEPART(WEEKDAY, dt) IN (1, 7) THEN 1 ELSE 0 END,
    CASE WHEN DAY(dt) = 1 THEN 1 ELSE 0 END,
    CASE WHEN dt = EOMONTH(dt) THEN 1 ELSE 0 END,
    CASE WHEN MONTH(dt) IN (3, 4) THEN 1 ELSE 0 END,
    CASE WHEN MONTH(dt) IN (4, 6) AND DAY(dt) >= 15 THEN 1 ELSE 0 END,
    CASE WHEN MONTH(dt) IN (5, 6, 7, 8) THEN 1 ELSE 0 END,
    CASE WHEN MONTH(dt) IN (11, 12, 1, 2) THEN 1 ELSE 0 END,
    CASE WHEN MONTH(dt) IN (3, 8, 9) THEN 1 ELSE 0 END,
    CASE WHEN DAY(dt) <= 7 THEN 1 ELSE 0 END,
    CASE WHEN (DATEPART(WEEKDAY, dt) + 5) % 7 = 4 THEN 1 ELSE 0 END,
    CASE WHEN DAY(dt) >= 25 THEN 1 ELSE 0 END,
    0, 0
FROM DateRange
OPTION (MAXRECURSION 2000);

SELECT 'dim_date rows:' AS info, COUNT(*) AS row_count FROM dim_date;
GO

-- POPULATE dim_product (33 families)
DELETE FROM dim_product;
GO

INSERT INTO dim_product (family, tajir_category, tajir_subcategory, is_fmcg, is_perishable)
VALUES
    ('GROCERY I',               'FMCG', 'Staples',          1, 0),
    ('GROCERY II',              'FMCG', 'Staples',          1, 0),
    ('BEVERAGES',               'FMCG', 'Beverages',        1, 0),
    ('CLEANING',                'FMCG', 'Home Care',        1, 0),
    ('PERSONAL CARE',           'FMCG', 'Personal Care',    1, 0),
    ('HOME CARE',               'FMCG', 'Home Care',        1, 0),
    ('BABY CARE',               'FMCG', 'Baby Care',        1, 0),
    ('PRODUCE',                 'Fresh', 'Vegetables',       1, 1),
    ('DAIRY',                   'Fresh', 'Dairy',            1, 1),
    ('BREAD/BAKERY',            'Fresh', 'Bakery',           1, 1),
    ('POULTRY',                 'Fresh', 'Poultry',          1, 1),
    ('MEATS',                   'Fresh', 'Meat',             1, 1),
    ('EGGS',                    'Fresh', 'Eggs',             1, 1),
    ('SEAFOOD',                 'Fresh', 'Seafood',          1, 1),
    ('DELI',                    'Fresh', 'Deli',             1, 1),
    ('FROZEN FOODS',            'Fresh', 'Frozen',           1, 0),
    ('PREPARED FOODS',          'Fresh', 'Ready Meals',      1, 1),
    ('LIQUOR,WINE,BEER',       'Non-Food', 'Beverages',     0, 0),
    ('HOME AND KITCHEN I',     'Non-Food', 'Household',      0, 0),
    ('HOME AND KITCHEN II',    'Non-Food', 'Household',      0, 0),
    ('HOME APPLIANCES',        'Non-Food', 'Appliances',     0, 0),
    ('HARDWARE',               'Non-Food', 'Hardware',       0, 0),
    ('LAWN AND GARDEN',        'Non-Food', 'Garden',         0, 0),
    ('BEAUTY',                 'Non-Food', 'Beauty',         0, 0),
    ('LINGERIE',               'Non-Food', 'Clothing',       0, 0),
    ('LADIESWEAR',             'Non-Food', 'Clothing',       0, 0),
    ('CELEBRATION',            'Non-Food', 'Gifts',          0, 0),
    ('PLAYERS AND ELECTRONICS','Non-Food', 'Electronics',    0, 0),
    ('AUTOMOTIVE',             'Non-Food', 'Automotive',     0, 0),
    ('PET SUPPLIES',           'Non-Food', 'Pet Care',       0, 0),
    ('SCHOOL AND OFFICE SUPPLIES','Non-Food', 'Stationery',  0, 0),
    ('MAGAZINES',              'Non-Food', 'Media',          0, 0),
    ('BOOKS',                  'Non-Food', 'Media',          0, 0);

SELECT 'dim_product rows:' AS info, COUNT(*) AS row_count FROM dim_product;
GO

-- VERIFY ALL TABLES
SELECT 'dim_date' AS table_name, COUNT(*) AS row_count FROM dim_date
UNION ALL SELECT 'dim_store', COUNT(*) FROM dim_store
UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL SELECT 'dim_economic', COUNT(*) FROM dim_economic
UNION ALL SELECT 'dim_pakistan_cpi', COUNT(*) FROM dim_pakistan_cpi
UNION ALL SELECT 'fact_daily_sales', COUNT(*) FROM fact_daily_sales;
GO
""")

# ============================================
# FILE 5: Analytical Queries
# ============================================
with open('sql/05_analytical_queries.sql', 'w') as f:
    f.write("""-- ============================================================
-- 10 ANALYTICAL QUERIES - Run AFTER loading data
-- ============================================================

USE TajirRetailDW;
GO

-- QUERY 1: Top 10 Products by Revenue
SELECT TOP 10
    p.family, p.tajir_category,
    FORMAT(SUM(f.sales), 'N0') AS total_revenue,
    FORMAT(AVG(f.sales), 'N2') AS avg_daily_sales
FROM fact_daily_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.family, p.tajir_category
ORDER BY SUM(f.sales) DESC;
GO

-- QUERY 2: Monthly Seasonal Pattern
SELECT
    d.month, d.month_name,
    FORMAT(AVG(f.sales), 'N2') AS avg_sales,
    FORMAT(CAST(SUM(CAST(f.is_zero_sale AS INT)) AS FLOAT) /
        NULLIF(COUNT(*), 0) * 100, 'N1') + '%%' AS stockout_rate
FROM fact_daily_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.month, d.month_name
ORDER BY d.month;
GO

-- QUERY 3: Ramadan Impact Analysis
SELECT
    CASE WHEN d.is_ramadan_period = 1 THEN 'Ramadan' ELSE 'Normal' END AS period,
    FORMAT(AVG(f.sales), 'N2') AS avg_sales,
    FORMAT(SUM(f.sales), 'N0') AS total_sales,
    FORMAT(CAST(SUM(CAST(f.is_zero_sale AS INT)) AS FLOAT) /
        NULLIF(COUNT(*), 0) * 100, 'N1') + '%%' AS stockout_rate
FROM fact_daily_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.is_ramadan_period
ORDER BY d.is_ramadan_period DESC;
GO

-- QUERY 4: Top 20 Stockout Hotspots
SELECT TOP 20
    s.store_id, s.city, s.store_type, p.family,
    COUNT(*) AS total_days,
    SUM(CAST(f.is_zero_sale AS INT)) AS zero_sale_days,
    FORMAT(CAST(SUM(CAST(f.is_zero_sale AS INT)) AS FLOAT) /
        NULLIF(COUNT(*), 0) * 100, 'N1') + '%%' AS stockout_rate
FROM fact_daily_sales f
JOIN dim_store s ON f.store_key = s.store_key
JOIN dim_product p ON f.product_key = p.product_key
WHERE p.is_fmcg = 1
GROUP BY s.store_id, s.city, s.store_type, p.family
HAVING CAST(SUM(CAST(f.is_zero_sale AS INT)) AS FLOAT) / NULLIF(COUNT(*), 0) > 0.3
ORDER BY SUM(CAST(f.is_zero_sale AS INT)) DESC;
GO

-- QUERY 5: Store Performance Ranking
SELECT
    s.store_id, s.city, s.store_type, s.store_size,
    FORMAT(SUM(f.sales), 'N0') AS total_revenue,
    FORMAT(AVG(f.sales), 'N2') AS avg_daily_sales,
    RANK() OVER (ORDER BY SUM(f.sales) DESC) AS revenue_rank
FROM fact_daily_sales f
JOIN dim_store s ON f.store_key = s.store_key
GROUP BY s.store_id, s.city, s.store_type, s.store_size
ORDER BY SUM(f.sales) DESC;
GO

-- QUERY 6: Weekend vs Weekday Analysis
SELECT
    CASE WHEN d.is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    FORMAT(AVG(f.sales), 'N2') AS avg_sales,
    FORMAT(SUM(f.sales), 'N0') AS total_sales
FROM fact_daily_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.is_weekend
ORDER BY d.is_weekend;
GO

-- QUERY 7: Promotion Effectiveness
SELECT
    p.family, p.tajir_category,
    FORMAT(AVG(CASE WHEN f.onpromotion = 1 THEN f.sales END), 'N2') AS avg_with_promo,
    FORMAT(AVG(CASE WHEN f.onpromotion = 0 THEN f.sales END), 'N2') AS avg_without_promo,
    FORMAT(
        (AVG(CASE WHEN f.onpromotion = 1 THEN f.sales END) -
         AVG(CASE WHEN f.onpromotion = 0 THEN f.sales END)) /
        NULLIF(AVG(CASE WHEN f.onpromotion = 0 THEN f.sales END), 0) * 100,
    'N1') + '%%' AS promotion_lift
FROM fact_daily_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.family, p.tajir_category
HAVING AVG(CASE WHEN f.onpromotion = 1 THEN f.sales END) IS NOT NULL
ORDER BY (AVG(CASE WHEN f.onpromotion = 1 THEN f.sales END) -
     AVG(CASE WHEN f.onpromotion = 0 THEN f.sales END)) /
    NULLIF(AVG(CASE WHEN f.onpromotion = 0 THEN f.sales END), 0) DESC;
GO

-- QUERY 8: Year-Over-Year Growth
;WITH YearlySales AS (
    SELECT d.year, SUM(f.sales) AS total_sales,
           AVG(f.sales) AS avg_sales,
           COUNT(DISTINCT d.full_date) AS active_days
    FROM fact_daily_sales f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY d.year
)
SELECT year,
    FORMAT(total_sales, 'N0') AS total_sales,
    FORMAT(avg_sales, 'N2') AS avg_daily_sales,
    active_days,
    FORMAT((total_sales - LAG(total_sales) OVER (ORDER BY year)) /
        NULLIF(LAG(total_sales) OVER (ORDER BY year), 0) * 100,
    'N1') + '%%' AS yoy_growth
FROM YearlySales
ORDER BY year;
GO

-- QUERY 9: Payday Effect Analysis
SELECT
    CASE
        WHEN d.is_payday_week = 1 THEN 'Payday Week (1-7)'
        WHEN d.is_end_of_month = 1 THEN 'End of Month (25-31)'
        ELSE 'Mid Month (8-24)'
    END AS period,
    FORMAT(AVG(f.sales), 'N2') AS avg_sales,
    FORMAT(SUM(f.sales), 'N0') AS total_sales
FROM fact_daily_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY CASE
        WHEN d.is_payday_week = 1 THEN 'Payday Week (1-7)'
        WHEN d.is_end_of_month = 1 THEN 'End of Month (25-31)'
        ELSE 'Mid Month (8-24)'
    END
ORDER BY AVG(f.sales) DESC;
GO

-- QUERY 10: Reorder Alerts
SELECT TOP 30
    s.store_id, s.city, p.family, p.tajir_category,
    f.consecutive_zeros AS days_without_sale,
    FORMAT(f.sales_rolling_mean_7d, 'N2') AS normal_daily_demand,
    FORMAT(f.sales_rolling_mean_7d * 7, 'N0') AS suggested_order_qty,
    'REORDER NOW' AS alert_status
FROM fact_daily_sales f
JOIN dim_store s ON f.store_key = s.store_key
JOIN dim_product p ON f.product_key = p.product_key
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.consecutive_zeros >= 3
  AND p.is_fmcg = 1
  AND f.sales_rolling_mean_7d > 10
  AND d.full_date = (SELECT MAX(full_date) FROM dim_date WHERE date_key IN
                     (SELECT date_key FROM fact_daily_sales))
ORDER BY f.sales_rolling_mean_7d DESC;
GO

SELECT 'All 10 analytical queries executed successfully!' AS status;
GO
""")

print("=" * 60)
print("✅ ALL 5 SQL FILES CREATED!")
print("=" * 60)

for f in os.listdir('sql'):
    size = os.path.getsize(os.path.join('sql', f)) / 1024
    print(f"  ✅ sql/{f:<35s} ({size:.1f} KB)")

print(f"""
{'=' * 60}
📋 NOW DO THIS IN ORDER:
{'=' * 60}

  STEP 1: Run verify script again
          python src/verify_warehouse.py

  STEP 2: Open SSMS and run IN ORDER:
          sql/01_create_database.sql      → Press F5
          sql/02_dimension_tables.sql     → Press F5
          sql/03_fact_tables.sql          → Press F5
          sql/04_populate_dimensions.sql  → Press F5

  STEP 3: Load real data from Python:
          python src/load_warehouse.py

  STEP 4: THEN run analytical queries:
          sql/05_analytical_queries.sql   → Press F5
""")