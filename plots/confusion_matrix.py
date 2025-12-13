"""
Confusion Matrix Heatmaps for PhD Admissions Models
"""

import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

cm_logistic = np.array([[162, 140], [40, 58]])
cm_gb_admission = np.array([[1707, 241], [407, 334]])


fig, axes = plt.subplots(1, 2, figsize=(14, 6))

# Logistic Regression
sns.heatmap(cm_logistic, annot=True, fmt='d', cmap='Blues',
            xticklabels=['No Offer', 'Offer'],
            yticklabels=['No Offer', 'Offer'],
            ax=axes[0], cbar_kws={'label': 'Count'})
axes[0].set_title('Logistic Regression\nPhD Admission Prediction (Test Set)', fontsize=12, fontweight='bold')
axes[0].set_ylabel('True Label', fontsize=11)
axes[0].set_xlabel('Predicted Label', fontsize=11)

# Gradient Boosting
sns.heatmap(cm_gb_admission, annot=True, fmt='d', cmap='Greens',
            xticklabels=['No Offer', 'Offer'],
            yticklabels=['No Offer', 'Offer'],
            ax=axes[1], cbar_kws={'label': 'Count'})
axes[1].set_title('Gradient Boosting\nPhD Admission Prediction (Test Set)', fontsize=12, fontweight='bold')
axes[1].set_ylabel('True Label', fontsize=11)
axes[1].set_xlabel('Predicted Label', fontsize=11)

plt.tight_layout()
plt.savefig('confusion_matrices.png', dpi=300, bbox_inches='tight')
plt.show()