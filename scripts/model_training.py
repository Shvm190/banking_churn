import pandas as pd
import sqlite3
import os
import joblib
import mlflow
import argparse
import logging
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import (
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    precision_recall_curve,
    auc,
    confusion_matrix
)
from datetime import datetime

def train_and_log_models(db_path):
    """
    Trains and evaluates a Random Forest model and logs it using MLflow.
    """
    # 1. Data Loading and Splitting
    logging.info("--- Starting Model Training and Evaluation ---")
    logging.info("Loading data from database...")
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query("SELECT * FROM customer_features", conn)
        conn.close()
    except Exception as e:
        logging.info(f"Error loading data: {e}")
        return

    X = df.drop('Churn_Yes', axis=1)
    y = df['Churn_Yes']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    print("Data split into training and testing sets.")

    # Identify categorical columns for preprocessing
    categorical_cols = X_train.select_dtypes(include='object').columns
    preprocessor = ColumnTransformer(
        transformers=[('onehot', OneHotEncoder(handle_unknown='ignore'), categorical_cols)],
        remainder='passthrough'
    )

    # 2. MLflow Experiment Setup
    mlflow.set_experiment("Churn Prediction RF Model Training")

    # 3. Train and Log Random Forest Model
    with mlflow.start_run(run_name="Random Forest"):
        print("Training Random Forest model...")
        rf_model = Pipeline(steps=[
            ('preprocessor', preprocessor),
            ('classifier', RandomForestClassifier(random_state=42))
        ])
        rf_model.fit(X_train, y_train)

        y_pred_rf = rf_model.predict(X_test)
        y_pred_proba = rf_model.predict_proba(X_test)[:, 1]

        # Calculate metrics
        precision_rf = precision_score(y_test, y_pred_rf, pos_label=1)
        recall_rf = recall_score(y_test, y_pred_rf, pos_label=1)
        f1_rf = f1_score(y_test, y_pred_rf, pos_label=1)

        # New metrics
        roc_auc_rf = roc_auc_score(y_test, y_pred_proba)

        precision_points, recall_points, _ = precision_recall_curve(y_test, y_pred_proba)
        pr_auc_rf = auc(recall_points, precision_points)

        cm = confusion_matrix(y_test, y_pred_rf)
        tn, fp, fn, tp = cm.ravel()
        specificity_rf = tn / (tn + fp)

        # Log parameters and metrics
        mlflow.log_param("model_type", "Random Forest")
        mlflow.log_param("random_state", 42)
        mlflow.log_metric("precision", precision_rf)
        mlflow.log_metric("recall", recall_rf)
        mlflow.log_metric("f1_score", f1_rf)

        # Log new metrics
        mlflow.log_metric("roc_auc", roc_auc_rf)
        mlflow.log_metric("pr_auc", pr_auc_rf)
        mlflow.log_metric("specificity", specificity_rf)

        # Log the model artifact
        mlflow.sklearn.log_model(rf_model, "random_forest_model")
        print("Random Forest model logged to MLflow.")

    print("--- Model Training and Evaluation Complete ---")

if __name__ == "__main__":

    # Generate filename with current datetime
    log_filename = f"logs/model_training_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename=log_filename,  # write logs to file
        filemode='a'             # append mode
    )
    
    parser = argparse.ArgumentParser(description="Train and log ML models.")
    parser.add_argument('--db-path', type=str, required=True, help='Path to the SQLite database.')
    args = parser.parse_args()

    train_and_log_models(args.db_path)