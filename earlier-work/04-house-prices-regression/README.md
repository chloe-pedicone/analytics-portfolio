# House prices: EDA and linear, Lasso and Ridge regression (Python)

**Type:** Kaggle notebook · **Tools:** Python (pandas, scikit-learn)
**Notebook:** [`house-prices-linreg-lasso-ridge.ipynb`](house-prices-linreg-lasso-ridge.ipynb) · [Original on Kaggle](https://www.kaggle.com/code/yangle2912/house-eda-linreg-lasso-ridge)
**Data:** Kaggle House Prices competition (public).

## What I did
- Explored the data and prepared features, then compared linear, Lasso and Ridge regression with 10-fold cross-validation.
- Result: training R² 0.88 vs test R² 0.72.

## What I'd do differently now
The train/test gap points to overfitting. Next time I'd use a log-transformed target, pipeline-based preprocessing to avoid leakage, and tuned regularization strength.
