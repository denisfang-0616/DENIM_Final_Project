"""
Gradient Boosting for PhD Admissions chances
"""

import pandas as pd
import numpy as np
import psycopg2
from dotenv import load_dotenv
import os
from sklearn.model_selection import train_test_split, GridSearchCV, StratifiedKFold
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, confusion_matrix, mean_squared_error, mean_absolute_error

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

TARGET = 'got_phd_offer'

conn = psycopg2.connect(**db_params)
query = f"SELECT {', '.join(FEATURES + [TARGET])} FROM admissions_data_cleaned WHERE {TARGET} IS NOT NULL"
df = pd.read_sql(query, conn)
conn.close()

X = df[FEATURES]
y = df[TARGET]
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.3, random_state=42, stratify=y
)

param_grid = {
    'max_depth': [3, 5, 7],
    'learning_rate': [0.01, 0.05, 0.1],
    'max_iter': [100, 200, 300]
}

model = HistGradientBoostingClassifier(random_state=42)
grid_search = GridSearchCV(
    model,
    param_grid,
    cv=StratifiedKFold(n_splits=5, shuffle=True, random_state=42),
    scoring='roc_auc',
    n_jobs=-1
)
grid_search.fit(X_train, y_train)

best_model = grid_search.best_estimator_

y_train_pred = best_model.predict(X_train)
y_test_pred = best_model.predict(X_test)
y_train_proba = best_model.predict_proba(X_train)[:, 1]
y_test_proba = best_model.predict_proba(X_test)[:, 1]

train_acc = accuracy_score(y_train, y_train_pred)
test_acc = accuracy_score(y_test, y_test_pred)
train_prec = precision_score(y_train, y_train_pred)
test_prec = precision_score(y_test, y_test_pred)
train_rec = recall_score(y_train, y_train_pred)
test_rec = recall_score(y_test, y_test_pred)
train_f1 = f1_score(y_train, y_train_pred)
test_f1 = f1_score(y_test, y_test_pred)
train_auc = roc_auc_score(y_train, y_train_proba)
test_auc = roc_auc_score(y_test, y_test_proba)
train_mse = mean_squared_error(y_train, y_train_pred)
test_mse = mean_squared_error(y_test, y_test_pred)
train_mae = mean_absolute_error(y_train, y_train_pred)
test_mae = mean_absolute_error(y_test, y_test_pred)

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
print(f"Percentage that recieved offer: {y.mean()*100:.1f}%")

print(f"\nBest parameters:")
print(f"  max_depth: {grid_search.best_params_['max_depth']}")
print(f"  learning_rate: {grid_search.best_params_['learning_rate']}")
print(f"  max_iter: {grid_search.best_params_['max_iter']}")

print("\n")
print("MODEL PERFORMANCE")
print(f"{'Metric':<20} {'Train':<15} {'Test':<15}")
print("-"*80)
print(f"{'Accuracy':<20} {train_acc:<15.4f} {test_acc:<15.4f}")
print(f"{'Precision':<20} {train_prec:<15.4f} {test_prec:<15.4f}")
print(f"{'Recall':<20} {train_rec:<15.4f} {test_rec:<15.4f}")
print(f"{'F1-Score':<20} {train_f1:<15.4f} {test_f1:<15.4f}")
print(f"{'AUC-ROC':<20} {train_auc:<15.4f} {test_auc:<15.4f}")
print(f"{'MSE':<20} {train_mse:<15.4f} {test_mse:<15.4f}")
print(f"{'MAE':<20} {train_mae:<15.4f} {test_mae:<15.4f}")

print("\n")
print("CONFUSION MATRIX (Test)")
cm = confusion_matrix(y_test, y_test_pred)
print(f"True Negatives:  {cm[0,0]:<10} | False Positives: {cm[0,1]}")
print(f"False Negatives: {cm[1,0]:<10} | True Positives:  {cm[1,1]}")

print("\n")
print("FEATURE IMPORTANCE")
print(feature_importance.to_string(index=False))