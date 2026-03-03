-- ============================================================
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
