-- ============================================================
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
