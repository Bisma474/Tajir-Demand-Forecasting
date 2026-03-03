"""
DAYS 7-8: Demand Forecasting Model
Two approaches: Prophet (time-series) + Gradient Boosting (ML)
Following the 15-day roadmap exactly.

Run: python src/demand_forecasting.py
"""
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os
import time
import pickle
import warnings
warnings.filterwarnings('ignore')

from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import cross_val_score

os.makedirs('models', exist_ok=True)
os.makedirs('screenshots', exist_ok=True)
os.makedirs('data/processed', exist_ok=True)

# Check if Prophet is available
try:
    from prophet import Prophet
    HAS_PROPHET = True
    print("Prophet installed: YES")
except ImportError:
    HAS_PROPHET = False
    print("Prophet installed: NO (will skip Prophet model)")

# Check if XGBoost is available
try:
    from xgboost import XGBRegressor
    HAS_XGBOOST = True
    print("XGBoost installed: YES")
except ImportError:
    HAS_XGBOOST = False
    print("XGBoost installed: NO (will use sklearn GradientBoosting)")


class DemandForecaster:
    """
    Demand Forecasting with TWO approaches:
      1. Gradient Boosting / XGBoost (ML approach)
      2. Prophet (Time-series approach)
    
    Both models are trained, evaluated, and compared.
    """

    def __init__(self):
        self.gb_model = None
        self.prophet_models = {}
        self.feature_columns = []
        self.gb_metrics = {}
        self.prophet_metrics = {}
        self.all_results = {}

    # ============================================
    # LOAD AND PREPARE DATA
    # ============================================
    def load_data(self):
        """Load ML-ready data from ETL pipeline or cleaned CSV."""
        print("\n" + "=" * 60)
        print("STEP 1: LOADING DATA")
        print("=" * 60)

        # Try ML-ready data first, then fallback
        if os.path.exists('data/processed/ml_ready_data.csv'):
            path = 'data/processed/ml_ready_data.csv'
        elif os.path.exists('data/cleaned/featured_retail_data.csv'):
            path = 'data/cleaned/featured_retail_data.csv'
        else:
            print("ERROR: No data found! Run ETL pipeline first.")
            return None

        print(f"   Loading: {path}")
        start = time.time()
        df = pd.read_csv(path, parse_dates=['date'])
        print(f"   Loaded: {len(df):,} rows, {len(df.columns)} columns")
        print(f"   Time: {time.time()-start:.1f}s")
        print(f"   Date range: {df['date'].min()} to {df['date'].max()}")

        return df

    def prepare_features(self, df):
        """Prepare features for Gradient Boosting model."""
        print("\n" + "=" * 60)
        print("STEP 2: PREPARING FEATURES")
        print("=" * 60)

        # Add encoded columns if missing
        if 'store_type_encoded' not in df.columns and 'store_type' in df.columns:
            store_type_map = {'A': 5, 'B': 4, 'C': 1, 'D': 3, 'E': 2}
            df['store_type_encoded'] = df['store_type'].map(store_type_map).fillna(0).astype(int)

        if 'family_encoded' not in df.columns and 'family' in df.columns:
            family_rank = df.groupby('family')['sales'].sum().rank(ascending=False)
            df['family_encoded'] = df['family'].map(family_rank).fillna(0).astype(int)

        if 'category_encoded' not in df.columns and 'tajir_category' in df.columns:
            cat_map = {'FMCG': 3, 'Fresh': 2, 'Non-Food': 1}
            df['category_encoded'] = df['tajir_category'].map(cat_map).fillna(0).astype(int)

        # Add interaction features if missing
        if 'promo_weekend' not in df.columns:
            df['promo_weekend'] = df.get('onpromotion', 0) * df.get('is_weekend', 0)
        if 'ramadan_fmcg' not in df.columns:
            df['ramadan_fmcg'] = df.get('is_ramadan_period', 0) * df.get('is_fmcg', 0)
        if 'wedding_grocery' not in df.columns:
            df['wedding_grocery'] = df.get('is_wedding_season', 0) * (df.get('family', '') == 'GROCERY I').astype(int)
        if 'payday_spending' not in df.columns:
            df['payday_spending'] = df.get('is_payday_week', 0) * df.get('category_encoded', 0)

        # Feature list
        self.feature_columns = [
            'year', 'month', 'day_of_week', 'day_of_month',
            'week_of_year', 'quarter', 'day_of_year',
            'is_weekend', 'is_month_start', 'is_month_end',
            'is_ramadan_period', 'is_eid_preparation', 'is_summer_peak',
            'is_wedding_season', 'is_school_season', 'is_payday_week',
            'is_friday', 'is_end_of_month',
            'is_holiday', 'is_national_holiday',
            'sales_lag_7d', 'sales_lag_14d', 'sales_lag_28d',
            'sales_rolling_mean_7d', 'sales_rolling_mean_14d', 'sales_rolling_mean_30d',
            'sales_rolling_std_7d', 'sales_rolling_std_14d',
            'sales_trend_7d',
            'is_zero_sale', 'zero_sales_last_7d', 'consecutive_zeros',
            'store_type_encoded', 'family_encoded', 'category_encoded',
            'cluster',
            'store_avg_daily_sales', 'family_avg_sales', 'store_product_avg',
            'oil_price',
            'onpromotion', 'onpromotion_count',
            'promo_weekend', 'ramadan_fmcg', 'wedding_grocery', 'payday_spending',
            'is_fmcg', 'is_perishable'
        ]

        # Keep only available columns
        available = [c for c in self.feature_columns if c in df.columns]
        missing = [c for c in self.feature_columns if c not in df.columns]
        self.feature_columns = available

        if missing:
            print(f"   Missing features (skipped): {missing[:10]}...")
        print(f"   Using {len(self.feature_columns)} features")

        # Fill NaN
        df[self.feature_columns] = df[self.feature_columns].fillna(0)

        # Time-based split: Train on 2013-2016, Test on 2017
        train_mask = df['date'].dt.year < 2017
        test_mask = df['date'].dt.year >= 2017

        X_train = df.loc[train_mask, self.feature_columns]
        y_train = df.loc[train_mask, 'sales']
        X_test = df.loc[test_mask, self.feature_columns]
        y_test = df.loc[test_mask, 'sales']

        print(f"   Train set: {len(X_train):,} rows (2013-2016)")
        print(f"   Test set:  {len(X_test):,} rows (2017)")

        return X_train, X_test, y_train, y_test, df

    # ============================================
    # MODEL 1: GRADIENT BOOSTING / XGBOOST
    # ============================================
    def train_gradient_boosting(self, X_train, y_train, X_test, y_test):
        """Train Gradient Boosting model (or XGBoost if available)."""
        print("\n" + "=" * 60)
        print("MODEL 1: GRADIENT BOOSTING")
        print("=" * 60)

        if HAS_XGBOOST:
            print("   Using: XGBoost (faster, better)")
            self.gb_model = XGBRegressor(
                n_estimators=500,
                max_depth=8,
                learning_rate=0.05,
                subsample=0.8,
                colsample_bytree=0.8,
                min_child_weight=5,
                reg_alpha=0.1,
                reg_lambda=1.0,
                random_state=42,
                n_jobs=-1,
                verbosity=1
            )
        else:
            print("   Using: sklearn GradientBoostingRegressor")
            self.gb_model = GradientBoostingRegressor(
                n_estimators=300,
                max_depth=6,
                learning_rate=0.05,
                subsample=0.8,
                min_samples_leaf=10,
                random_state=42
            )

        print("   Training... (this may take 5-15 minutes)")
        start = time.time()
        self.gb_model.fit(X_train, y_train)
        train_time = time.time() - start
        print(f"   Trained in {train_time:.1f} seconds ({train_time/60:.1f} min)")

        # Predict
        y_pred = self.gb_model.predict(X_test)
        y_pred = np.clip(y_pred, 0, None)

        # Metrics
        mae = mean_absolute_error(y_test, y_pred)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        r2 = r2_score(y_test, y_pred)

        mask = y_test > 0
        if mask.sum() > 0:
            mape = np.mean(np.abs((y_test[mask] - y_pred[mask]) / y_test[mask])) * 100
        else:
            mape = 0

        self.gb_metrics = {
            'MAE': mae, 'RMSE': rmse, 'R2': r2,
            'MAPE': mape, 'Training_Time': train_time
        }

        print(f"\n   GRADIENT BOOSTING RESULTS:")
        print(f"   {'─' * 45}")
        print(f"   MAE  (Mean Absolute Error):     {mae:>10,.2f}")
        print(f"   RMSE (Root Mean Squared Error):  {rmse:>10,.2f}")
        print(f"   R2   (R-Squared Score):          {r2:>10.4f}")
        print(f"   MAPE (Mean Abs % Error):         {mape:>10.2f}%")

        # Feature importance
        if hasattr(self.gb_model, 'feature_importances_'):
            importance = pd.DataFrame({
                'feature': self.feature_columns,
                'importance': self.gb_model.feature_importances_
            }).sort_values('importance', ascending=False)

            print(f"\n   TOP 10 MOST IMPORTANT FEATURES:")
            print(f"   {'─' * 45}")
            for i, (_, row) in enumerate(importance.head(10).iterrows(), 1):
                bar = '#' * int(row['importance'] / importance['importance'].max() * 25)
                print(f"   {i:2d}. {row['feature']:<30s} {bar} {row['importance']:.4f}")

            importance.to_csv('data/processed/feature_importance.csv', index=False)

        # Save model
        with open('models/gradient_boosting_model.pkl', 'wb') as f:
            pickle.dump({
                'model': self.gb_model,
                'features': self.feature_columns,
                'metrics': self.gb_metrics
            }, f)
        print(f"\n   Model saved: models/gradient_boosting_model.pkl")

        return y_pred, importance

    # ============================================
    # MODEL 2: PROPHET (TIME-SERIES)
    # ============================================
    def train_prophet(self, df):
        """
        Train Prophet model for specific store-product combinations.
        Prophet works on individual time series, so we pick top combos.
        """
        print("\n" + "=" * 60)
        print("MODEL 2: PROPHET (TIME-SERIES)")
        print("=" * 60)

        if not HAS_PROPHET:
            print("   Prophet not installed. Skipping...")
            print("   This is OK — Gradient Boosting is the primary model.")
            print("   To install later: pip install prophet")
            self.prophet_metrics = {'status': 'skipped'}
            return

        # Pick top 3 store-product combinations by total sales
        top_combos = df.groupby(['store_id', 'family'])['sales'].sum() \
            .sort_values(ascending=False).head(3).index.tolist()

        print(f"   Training Prophet on {len(top_combos)} store-product combos:")

        prophet_results = []

        for store_id, family in top_combos:
            print(f"\n   Store {store_id} | {family}")

            # Filter data for this combo
            mask = (df['store_id'] == store_id) & (df['family'] == family)
            ts = df[mask][['date', 'sales']].copy()
            ts.columns = ['ds', 'y']
            ts = ts.sort_values('ds')

            # Train/test split (2013-2016 train, 2017 test)
            train = ts[ts['ds'].dt.year < 2017]
            test = ts[ts['ds'].dt.year >= 2017]

            if len(train) < 100 or len(test) < 10:
                print(f"      Skipped (not enough data)")
                continue

            # Train Prophet
            model = Prophet(
                yearly_seasonality=True,
                weekly_seasonality=True,
                daily_seasonality=False,
                changepoint_prior_scale=0.05
            )
            model.fit(train)

            # Predict
            future = model.make_future_dataframe(periods=len(test))
            forecast = model.predict(future)

            # Get predictions for test period only
            pred_test = forecast[forecast['ds'].isin(test['ds'])]['yhat'].values
            actual_test = test['y'].values[:len(pred_test)]

            if len(pred_test) == 0 or len(actual_test) == 0:
                print(f"      Skipped (no overlapping predictions)")
                continue

            # Clip negatives
            pred_test = np.clip(pred_test, 0, None)

            # Metrics
            mae = mean_absolute_error(actual_test, pred_test)
            rmse = np.sqrt(mean_squared_error(actual_test, pred_test))
            r2 = r2_score(actual_test, pred_test)

            print(f"      MAE: {mae:.2f} | RMSE: {rmse:.2f} | R2: {r2:.4f}")

            prophet_results.append({
                'store_id': store_id,
                'family': family,
                'MAE': mae, 'RMSE': rmse, 'R2': r2,
                'train_size': len(train),
                'test_size': len(actual_test)
            })

            # Save Prophet model for this combo
            self.prophet_models[f"store{store_id}_{family}"] = model

        if prophet_results:
            self.prophet_metrics = pd.DataFrame(prophet_results)
            self.prophet_metrics.to_csv('data/processed/prophet_metrics.csv', index=False)
            print(f"\n   Prophet metrics saved: data/processed/prophet_metrics.csv")

            avg_mae = self.prophet_metrics['MAE'].mean()
            avg_r2 = self.prophet_metrics['R2'].mean()
            print(f"\n   PROPHET AVERAGE RESULTS:")
            print(f"   {'─' * 45}")
            print(f"   Avg MAE:  {avg_mae:.2f}")
            print(f"   Avg R2:   {avg_r2:.4f}")

    # ============================================
    # COMPARE MODELS
    # ============================================
    def compare_models(self):
        """Compare Gradient Boosting vs Prophet results."""
        print("\n" + "=" * 60)
        print("MODEL COMPARISON")
        print("=" * 60)

        print(f"\n   {'Metric':<25s} {'Gradient Boosting':>20s}", end="")
        if isinstance(self.prophet_metrics, pd.DataFrame):
            print(f" {'Prophet (avg)':>20s}")
        else:
            print(f" {'Prophet':>20s}")

        print(f"   {'─' * 65}")

        gb_mae = self.gb_metrics.get('MAE', 0)
        gb_rmse = self.gb_metrics.get('RMSE', 0)
        gb_r2 = self.gb_metrics.get('R2', 0)

        if isinstance(self.prophet_metrics, pd.DataFrame) and len(self.prophet_metrics) > 0:
            p_mae = self.prophet_metrics['MAE'].mean()
            p_rmse = self.prophet_metrics['RMSE'].mean()
            p_r2 = self.prophet_metrics['R2'].mean()

            print(f"   {'MAE':<25s} {gb_mae:>20,.2f} {p_mae:>20,.2f}")
            print(f"   {'RMSE':<25s} {gb_rmse:>20,.2f} {p_rmse:>20,.2f}")
            print(f"   {'R2':<25s} {gb_r2:>20.4f} {p_r2:>20.4f}")

            winner = "Gradient Boosting" if gb_r2 > p_r2 else "Prophet"
            print(f"\n   WINNER: {winner}")
        else:
            print(f"   {'MAE':<25s} {gb_mae:>20,.2f} {'N/A':>20s}")
            print(f"   {'RMSE':<25s} {gb_rmse:>20,.2f} {'N/A':>20s}")
            print(f"   {'R2':<25s} {gb_r2:>20.4f} {'N/A':>20s}")
            print(f"\n   Prophet was skipped - Gradient Boosting is the primary model")

    # ============================================
    # GENERATE ALL CHARTS
    # ============================================
    def generate_charts(self, y_test, y_pred, importance):
        """Generate evaluation charts for screenshots."""
        print("\n" + "=" * 60)
        print("GENERATING CHARTS")
        print("=" * 60)

        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Demand Forecasting Model Results\nTajir Retail - Kiryana Store Optimization',
                     fontsize=16, fontweight='bold')

        # Chart 1: Actual vs Predicted
        sample_idx = np.random.choice(len(y_test), min(10000, len(y_test)), replace=False)
        y_test_s = y_test.iloc[sample_idx]
        y_pred_s = y_pred[sample_idx]

        axes[0, 0].scatter(y_test_s, y_pred_s, alpha=0.1, s=5, color='#2E86C1')
        max_val = max(y_test_s.max(), y_pred_s.max())
        axes[0, 0].plot([0, max_val], [0, max_val], 'r--', linewidth=2, label='Perfect')
        axes[0, 0].set_title(f'Actual vs Predicted (R2={self.gb_metrics["R2"]:.3f})')
        axes[0, 0].set_xlabel('Actual Sales')
        axes[0, 0].set_ylabel('Predicted Sales')
        axes[0, 0].legend()
        axes[0, 0].grid(True, alpha=0.3)

        # Chart 2: Feature Importance Top 15
        top_15 = importance.head(15).sort_values('importance', ascending=True)
        colors = ['#E74C3C' if any(k in f for k in ['ramadan', 'eid', 'wedding', 'payday', 'friday', 'summer'])
                  else '#2E86C1' for f in top_15['feature']]
        top_15.plot(x='feature', y='importance', kind='barh', ax=axes[0, 1],
                   color=colors, legend=False)
        axes[0, 1].set_title('Top 15 Feature Importance\n(Red = Pakistan-specific features)')
        axes[0, 1].set_xlabel('Importance')

        # Chart 3: Error Distribution
        errors = y_test.values - y_pred
        axes[1, 0].hist(errors, bins=100, color='#27AE60', alpha=0.7, edgecolor='white')
        axes[1, 0].axvline(x=0, color='red', linestyle='--', linewidth=2)
        axes[1, 0].set_title(f'Prediction Error Distribution (MAE={self.gb_metrics["MAE"]:.1f})')
        axes[1, 0].set_xlabel('Error (Actual - Predicted)')
        axes[1, 0].set_ylabel('Frequency')
        axes[1, 0].set_xlim(-500, 500)

        # Chart 4: Model Metrics Summary
        metrics_names = ['MAE', 'RMSE', 'R2', 'MAPE']
        metrics_values = [self.gb_metrics['MAE'], self.gb_metrics['RMSE'],
                         self.gb_metrics['R2'], self.gb_metrics['MAPE']]

        bars = axes[1, 1].barh(metrics_names, metrics_values, color=['#3498DB', '#E67E22', '#2ECC71', '#9B59B6'])
        axes[1, 1].set_title('Model Performance Metrics')
        for bar, val in zip(bars, metrics_values):
            axes[1, 1].text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2,
                           f'{val:.2f}', va='center', fontweight='bold')

        plt.tight_layout()
        plt.savefig('screenshots/day7_model_results.png', dpi=150, bbox_inches='tight')
        print(f"   Saved: screenshots/day7_model_results.png")

        # Chart 5: Actual vs Predicted over time
        fig2, ax2 = plt.subplots(1, 1, figsize=(16, 6))
        fig2.suptitle('Actual vs Predicted Daily Sales (2017 Test Period)', fontsize=14, fontweight='bold')

        # Aggregate by day for readability
        n_groups = min(200, len(y_test))
        group_size = len(y_test) // n_groups

        daily_actual = [y_test.values[i*group_size:(i+1)*group_size].mean() for i in range(n_groups)]
        daily_pred = [y_pred[i*group_size:(i+1)*group_size].mean() for i in range(n_groups)]

        ax2.plot(range(n_groups), daily_actual, label='Actual', color='#2E86C1', linewidth=1.5)
        ax2.plot(range(n_groups), daily_pred, label='Predicted', color='#E74C3C', linewidth=1.5, alpha=0.8)
        ax2.fill_between(range(n_groups), daily_actual, daily_pred, alpha=0.15, color='gray')
        ax2.legend(fontsize=12)
        ax2.set_ylabel('Average Sales')
        ax2.set_xlabel('Time Periods (2017)')
        ax2.grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig('screenshots/day7_prediction_timeline.png', dpi=150, bbox_inches='tight')
        print(f"   Saved: screenshots/day7_prediction_timeline.png")

        # Chart 6: Feature importance detailed
        fig3, ax3 = plt.subplots(1, 1, figsize=(12, 10))
        top_25 = importance.head(25).sort_values('importance', ascending=True)
        colors = ['#E74C3C' if any(k in f for k in ['ramadan', 'eid', 'wedding', 'payday', 'friday', 'summer', 'school'])
                  else '#3498DB' for f in top_25['feature']]
        ax3.barh(top_25['feature'], top_25['importance'], color=colors)
        ax3.set_title('Feature Importance — Top 25 Features\n(Red = Pakistan-Specific Features)', fontsize=14)
        ax3.set_xlabel('Importance Score')
        plt.tight_layout()
        plt.savefig('screenshots/day7_feature_importance.png', dpi=150, bbox_inches='tight')
        print(f"   Saved: screenshots/day7_feature_importance.png")


# ============================================
# MAIN
# ============================================
if __name__ == "__main__":
    print("=" * 60)
    print("   DAYS 7-8: DEMAND FORECASTING MODEL")
    print("   Two Approaches: Gradient Boosting + Prophet")
    print("=" * 60)

    forecaster = DemandForecaster()

    # Step 1: Load data
    df = forecaster.load_data()
    if df is None:
        exit(1)

    # Step 2: Prepare features
    X_train, X_test, y_train, y_test, df = forecaster.prepare_features(df)

    # Step 3: Train Gradient Boosting
    y_pred, importance = forecaster.train_gradient_boosting(X_train, y_train, X_test, y_test)

    # Step 4: Train Prophet (on top 3 store-product combos)
    forecaster.train_prophet(df)

    # Step 5: Compare models
    forecaster.compare_models()

    # Step 6: Generate charts
    forecaster.generate_charts(y_test, y_pred, importance)

    # Step 7: Save predictions
    predictions = pd.DataFrame({
        'actual': y_test.values,
        'predicted': y_pred
    })
    predictions.to_csv('data/processed/predictions.csv', index=False)
    print(f"\n   Predictions saved: data/processed/predictions.csv")

    # Final summary
    print(f"\n{'=' * 60}")
    print(f"   DAYS 7-8 COMPLETE!")
    print(f"{'=' * 60}")
    print(f"""
   MODEL 1 - Gradient Boosting:
      MAE:  {forecaster.gb_metrics['MAE']:,.2f}
      RMSE: {forecaster.gb_metrics['RMSE']:,.2f}
      R2:   {forecaster.gb_metrics['R2']:.4f}
      MAPE: {forecaster.gb_metrics['MAPE']:.2f}%

   FILES SAVED:
      models/gradient_boosting_model.pkl
      data/processed/feature_importance.csv
      data/processed/predictions.csv
      screenshots/day7_model_results.png
      screenshots/day7_prediction_timeline.png
      screenshots/day7_feature_importance.png

   CHECKLIST:
      [x] Gradient Boosting model trained and evaluated
      [x] Prophet model trained (if installed)
      [x] Model metrics recorded (MAE, RMSE, R2)
      [x] Feature importance chart generated
      [x] Model saved as .pkl file
      [x] Predictions saved

   NEXT: Day 9-10 - Stockout Analysis + Reorder Alert System
""")