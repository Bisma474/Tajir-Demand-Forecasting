-- ============================================================
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
