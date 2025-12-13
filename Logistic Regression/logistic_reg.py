import pandas as pd
import numpy as np
import psycopg2
from dotenv import load_dotenv
import os
from sklearn.model_selection import train_test_split, GridSearchCV, StratifiedKFold
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, confusion_matrix, classification_report

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
query = f"SELECT {', '.join(FEATURES + [TARGET])} FROM admissions_data_cleaned"
df = pd.read_sql(query, conn)
conn.close()

df_complete = df.dropna()

X = df_complete[FEATURES]
y = df_complete[TARGET]
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.3, random_state=42, stratify=y
)

#GridsearchCV/parameter optimization
param_grid = {'C': [0.01, 0.1, 1, 10, 100, 1000]}
logit = LogisticRegression(
    penalty='l2', 
    solver='lbfgs', 
    max_iter=1000, 
    random_state=42,
    class_weight='balanced'  
)
grid_search = GridSearchCV(
    logit, 
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

train_accuracy = accuracy_score(y_train, y_train_pred)
test_accuracy = accuracy_score(y_test, y_test_pred)
train_precision = precision_score(y_train, y_train_pred)
test_precision = precision_score(y_test, y_test_pred)
train_recall = recall_score(y_train, y_train_pred)
test_recall = recall_score(y_test, y_test_pred)
train_f1 = f1_score(y_train, y_train_pred)
test_f1 = f1_score(y_test, y_test_pred)
train_auc = roc_auc_score(y_train, y_train_proba)
test_auc = roc_auc_score(y_test, y_test_proba)

print(f"\nDataset: {len(df_complete):,} observations")
print(f"Train: {len(X_train):,} | Test: {len(X_test):,}")
print(f"Percentage that recieved offer: {y.mean()*100:.1f}%")
print(f"Best C parameter: {grid_search.best_params_['C']}")

print("\n")
print("MODEL PERFORMANCE")
print(f"{'Metric':<20} {'Train':<15} {'Test':<15}")
print("-"*80)
print(f"{'Accuracy':<20} {train_accuracy:<15.4f} {test_accuracy:<15.4f}")
print(f"{'Precision':<20} {train_precision:<15.4f} {test_precision:<15.4f}")
print(f"{'Recall':<20} {train_recall:<15.4f} {test_recall:<15.4f}")
print(f"{'F1-Score':<20} {train_f1:<15.4f} {test_f1:<15.4f}")
print(f"{'AUC-ROC':<20} {train_auc:<15.4f} {test_auc:<15.4f}")

print("\n")
print("CONFUSION MATRIX (Test)")
cm = confusion_matrix(y_test, y_test_pred)
print(f"True Negatives:  {cm[0,0]:<10} | False Positives: {cm[0,1]}")
print(f"False Negatives: {cm[1,0]:<10} | True Positives:  {cm[1,1]}")

print("\n")
print("COEFFICIENTS & ODDS RATIOS")
coefficients = pd.DataFrame({
    'Feature': FEATURES,
    'Coefficient': best_model.coef_[0],
    'Odds_Ratio': np.exp(best_model.coef_[0])
}).sort_values('Coefficient', ascending=False)
print(coefficients.to_string(index=False))

