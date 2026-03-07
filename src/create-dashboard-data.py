"""Create smaller datasets for Streamlit Cloud deployment."""
import pandas as pd
import os

os.makedirs('data/cloud', exist_ok=True)

# 1. Main data - sample for cloud
print("Loading full data...")
df = pd.read_csv('data/processed/ml_ready_data.csv', parse_dates=['date'])
print(f"Full data: {len(df):,} rows ({os.path.getsize('data/processed/ml_ready_data.csv')/1024/1024:.0f} MB)")

# Keep ALL 2017 (test year) + 15% of 2013-2016
df_2017 = df[df['date'].dt.year >= 2017]
df_rest = df[df['date'].dt.year < 2017].sample(frac=0.15, random_state=42)
df_small = pd.concat([df_2017, df_rest]).sort_values('date').reset_index(drop=True)

df_small.to_csv('data/cloud/main_data.csv', index=False)
size = os.path.getsize('data/cloud/main_data.csv') / (1024 * 1024)
print(f"Cloud data: {len(df_small):,} rows ({size:.1f} MB)")

# 2. Copy small files directly
small_files = [
    ('data/processed/predictions.csv', 'data/cloud/predictions.csv'),
    ('data/processed/feature_importance.csv', 'data/cloud/feature_importance.csv'),
    ('data/processed/stockout_risk_assessment.csv', 'data/cloud/stockout_risk_assessment.csv'),
    ('data/processed/reorder_recommendations.csv', 'data/cloud/reorder_recommendations.csv'),
    ('data/processed/active_reorder_alerts.csv', 'data/cloud/active_reorder_alerts.csv'),
]

for src, dst in small_files:
    if os.path.exists(src):
        df_temp = pd.read_csv(src)
        df_temp.to_csv(dst, index=False)
        size = os.path.getsize(dst) / (1024 * 1024)
        print(f"Copied: {dst} ({size:.1f} MB)")
    else:
        print(f"Missing: {src} (skipped)")

# 3. Model metrics (save as CSV so no pickle needed on cloud)
try:
    import pickle
    for model_path in ['models/gradient_boosting_model.pkl', 'models/demand_forecaster.pkl']:
        if os.path.exists(model_path):
            with open(model_path, 'rb') as f:
                model_data = pickle.load(f)
            metrics = model_data.get('metrics', {})
            metrics_df = pd.DataFrame([metrics])
            metrics_df.to_csv('data/cloud/model_metrics.csv', index=False)
            print(f"Model metrics saved: data/cloud/model_metrics.csv")
            break
except Exception as e:
    print(f"Could not extract model metrics: {e}")

print("\nAll cloud data ready in data/cloud/")
print("Check sizes:")
for f in os.listdir('data/cloud'):
    size = os.path.getsize(f'data/cloud/{f}') / (1024 * 1024)
    print(f"  {f}: {size:.1f} MB")