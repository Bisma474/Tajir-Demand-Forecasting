# 🏪 Kiryana Store Demand Forecasting & Inventory Optimization

> An end-to-end cloud-deployed data science pipeline that predicts product demand for small retail stores and generates smart reorder alerts — solving the core inventory challenge faced by B2B retail platforms like [Tajir](https://tajir.app).

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![Azure SQL](https://img.shields.io/badge/Azure_SQL-Cloud_Warehouse-0078D4?logo=microsoftazure&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?logo=streamlit&logoColor=white)
![scikit-learn](https://img.shields.io/badge/scikit--learn-ML_Models-F7931E?logo=scikitlearn&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green)

## 📌 Problem Statement

Small retail stores (kiryana stores) in Pakistan face a critical inventory management challenge:
- **Overstocking** → Money locked in unsold inventory, products expire
- **Stockouts** → Lost sales, unhappy customers, customers go to competitors
- **No data-driven decisions** → Store owners rely on gut feeling

**This project solves this by predicting what products each store needs, when they need them, and how much to order.**

## 🏗️ Architecture

```
┌─────────────┐     ┌───────────┐     ┌────────────────┐     ┌────────────┐     ┌──────────────┐
│ Data Sources │────▶│ Python    │────▶│ Azure SQL DB   │────▶│ ML Models  │────▶│  Streamlit   │
│ Kaggle +     │     │ ETL       │     │ Star Schema    │     │ Prophet +  │     │  Dashboard   │
│ SBP Pakistan │     │ Pipeline  │     │ Data Warehouse │     │ GBR        │     │  (Cloud)     │
└─────────────┘     └───────────┘     └────────────────┘     └────────────┘     └──────────────┘
```

## 🔧 Tech Stack

| Component | Technology |
|-----------|-----------|
| ☁️ Cloud Database | Azure SQL Database (Star Schema) |
| 🔧 ETL Pipeline | Python, Pandas, PyODBC |
| 🧹 Data Cleaning | Deduplication, null handling, outlier capping, multi-source merging |
| 🤖 ML Models | Facebook Prophet (time-series) + Gradient Boosting (feature-based) |
| 📊 Dashboard | Streamlit + Plotly (deployed on Streamlit Cloud) |
| 📥 Data Sources | Kaggle Retail + SBP Pakistan FMCG + PBS CPI |

## 🇵🇰 Pakistan-Specific Features

This project models demand patterns unique to the Pakistani market:
| Feature | Impact |
|---------|--------|
| 🌙 Ramadan | FMCG demand spikes 30-80% |
| ☀️ Summer Heat | Beverages surge in Lahore's 45°C weather |
| 💒 Wedding Season | Cooking supplies explode Nov-Feb |
| 💰 Payday Effect | Higher spending in first week of month |
| 🕌 Eid Preparation | Massive demand 2 weeks before Eid |
| 🏫 School Season | Stationery & snacks spike Mar/Aug/Sep |

## 📊 Key Results

| Metric | Value |
|--------|-------|
| Gradient Boosting R² | *Coming soon* |
| Mean Absolute Error | *Coming soon* |
| Products Analyzed | 33 families |
| Stores Covered | 54 stores |
| Data Points | 3M+ transactions |

## 🚀 Quick Start

```bash
# Clone the repository
git clone https://github.com/bismahhhhi/tajir-demand-forecasting.git
cd tajir-demand-forecasting

# Install dependencies
pip install -r requirements.txt

# Copy config template and fill in your credentials
cp src/config_template.py src/config.py

# Run the pipeline
python src/data_cleaning.py
python src/feature_engineering.py
python src/etl_pipeline.py
python src/demand_forecasting.py
python src/stockout_analysis.py

# Launch the dashboard
streamlit run dashboard/app.py
```

## 📁 Project Structure

```
tajir-demand-forecasting/
���── data/
│   ├── raw/                    # Original Kaggle datasets
│   ├── cleaned/                # Processed & featured data
│   └── pakistan/               # SBP FMCG + PBS CPI data
├── sql/                        # Warehouse schema & queries
│   ├── 01_create_database.sql
│   ├── 02_dimension_tables.sql
│   ├── 03_fact_tables.sql
│   └── 04_analytical_queries.sql
├── src/                        # Python source code
│   ├── config_template.py      # Configuration template
│   ├── data_cleaning.py        # Data cleaning module
│   ├── feature_engineering.py  # Feature creation
│   ├── etl_pipeline.py         # ETL to Azure cloud
│   ├── demand_forecasting.py   # ML models
│   ├── stockout_analysis.py    # Stockout detection
│   └── reorder_alerts.py       # Alert engine
├── models/                     # Saved ML models
├── dashboard/
│   └── app.py                  # Streamlit dashboard
├── notebooks/                  # Exploration notebooks
├── screenshots/                # Project screenshots
├── requirements.txt
├── LICENSE
└── README.md
```

## 👤 Author

**Bismah** — Aspiring Data Scientist

Built as a Data Science internship project demonstrating end-to-end capabilities:
data cleaning → warehouse design → ETL → machine learning → cloud deployment
