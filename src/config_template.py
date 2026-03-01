"""
Configuration Template for Tajir Demand Forecasting Project.

INSTRUCTIONS:
1. Copy this file and rename to: config.py
2. Fill in your Azure SQL credentials
3. config.py is in .gitignore — it will NOT be pushed to GitHub
"""

# ============================================
# AZURE SQL DATABASE CREDENTIALS
# ============================================
AZURE_SERVER = "your-server-name.database.windows.net"
AZURE_DATABASE = "TajirRetailDW"
AZURE_USERNAME = "your_username"
AZURE_PASSWORD = "your_password"

AZURE_CONN_STRING = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={AZURE_SERVER};"
    f"DATABASE={AZURE_DATABASE};"
    f"UID={AZURE_USERNAME};"
    f"PWD={AZURE_PASSWORD};"
    f"Encrypt=yes;"
    f"TrustServerCertificate=no;"
    f"Connection Timeout=30;"
)

# ============================================
# LOCAL SQL SERVER (backup)
# ============================================
LOCAL_CONN_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=TajirRetailDW;"
    "Trusted_Connection=yes;"
)

# ============================================
# FILE PATHS
# ============================================
DATA_RAW = "data/raw/"
DATA_CLEANED = "data/cleaned/"
DATA_PAKISTAN = "data/pakistan/"
MODELS_PATH = "models/"
