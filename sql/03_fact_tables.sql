-- ============================================================
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
