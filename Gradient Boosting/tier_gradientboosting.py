"""
Gradient Boosting for PhD Placement Tier Prediction
"""

import pandas as pd
import numpy as np
import psycopg2
from dotenv import load_dotenv
import os
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

load_dotenv()

db_params = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT')),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

FEATURES = ['undergrad_gpa_std', 'attended_grad_program', 'taken_calculus', 
            'taken_linear_algebra', 'taken_real_analysis', 'gre_quant_std', 
            'gre_verbal_std', 'undergrad_econ_related', 'academic_lor', 
            'professional_lor', 'undergrad_rank']

TARGET = 'phd_accepted_rank'

conn = psycopg2.connect(**db_params)
query = f"SELECT {', '.join(FEATURES + [TARGET])} FROM admissions_data_cleaned WHERE {TARGET} IS NOT NULL"
df = pd.read_sql(query, conn)
conn.close()

X = df[FEATURES]
y = df[TARGET]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.3, random_state=42
)

param_grid = {
    'max_depth': [3, 5, 7],
    'learning_rate': [0.01, 0.05, 0.1],
    'max_iter': [100, 200, 300]
}

model = HistGradientBoostingRegressor(random_state=42)
grid_search = GridSearchCV(
    model,
    param_grid,
    cv=5,
    scoring='neg_mean_squared_error',
    n_jobs=-1
)
grid_search.fit(X_train, y_train)

best_model = grid_search.best_estimator_

y_train_pred = best_model.predict(X_train)
y_test_pred = best_model.predict(X_test)

train_mse = mean_squared_error(y_train, y_train_pred)
test_mse = mean_squared_error(y_test, y_test_pred)
train_rmse = np.sqrt(train_mse)
test_rmse = np.sqrt(test_mse)
train_mae = mean_absolute_error(y_train, y_train_pred)
test_mae = mean_absolute_error(y_test, y_test_pred)
train_r2 = r2_score(y_train, y_train_pred)
test_r2 = r2_score(y_test, y_test_pred)

from sklearn.inspection import permutation_importance

perm_importance = permutation_importance(
    best_model, X_test, y_test, n_repeats=10, random_state=42, n_jobs=-1
)

feature_importance = pd.DataFrame({
    'Feature': FEATURES,
    'Importance': perm_importance.importances_mean
}).sort_values('Importance', ascending=False)

print(f"\nDataset: {len(df):,} records (includes blank data)")
print(f"Train: {len(X_train):,} | Test: {len(X_test):,}")
print(f"Target range: 1 (Top 10) to 4 (Top 100)")
print(f"\nBest parameters:")
print(f"  max_depth: {grid_search.best_params_['max_depth']}")
print(f"  learning_rate: {grid_search.best_params_['learning_rate']}")
print(f"  max_iter: {grid_search.best_params_['max_iter']}")

print("\n")
print("MODEL PERFORMANCE")
print(f"{'Metric':<20} {'Train':<15} {'Test':<15}")
print("-"*80)
print(f"{'MSE':<20} {train_mse:<15.4f} {test_mse:<15.4f}")
print(f"{'RMSE':<20} {train_rmse:<15.4f} {test_rmse:<15.4f}")
print(f"{'MAE':<20} {train_mae:<15.4f} {test_mae:<15.4f}")
print(f"{'R²':<20} {train_r2:<15.4f} {test_r2:<15.4f}")

print("\n")
print("FEATURE IMPORTANCE")
print(feature_importance.to_string(index=False))