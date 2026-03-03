-- ============================================================
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
