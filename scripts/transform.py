# ---
# scripts/transform.py
# Deliverables: SQL schema, sample queries, and a transformed dataset in a database

import pandas as pd
import os
import sqlite3
import logging
from datetime import datetime


def transform_data():
    """
    Performs feature engineering and stores the transformed data in an SQLite database.
    """
    logging.info("Starting data transformation...")
    
    try:
        prepared_data_dir = os.path.join(os.path.dirname(__file__), '../data/prepared')
        processed_data_dir = os.path.join(os.path.dirname(__file__), '../data/processed')
        os.makedirs(processed_data_dir, exist_ok=True)
        
        cleaned_file_path = os.path.join(prepared_data_dir, 'customer_data_cleaned.csv')
        df = pd.read_csv(cleaned_file_path)

        # 1. Feature Engineering
        # Create aggregated features (e.g., avg_monthly_charge_per_tenure)
        # This is a key predictive feature for churn
        df['avg_monthly_charge_per_tenure'] = df['MonthlyCharges'] / (df['tenure'] + 1e-6)

        # The 'age' column does not exist in the Telco Churn dataset.
        # We use the existing 'SeniorCitizen' (0/1) column directly.
        # df['IsSeniorCitizen'] = df['age'].apply(lambda x: 1 if x >= 65 else 0)
        logging.info("Engineered new feature: 'avg_monthly_charge_per_tenure'.")

        # 2. Categorical variable encoding (One-Hot Encoding)
        # Identify non-numeric columns to encode
        categorical_cols = df.select_dtypes(include='object').columns.tolist()
        
        # Remove 'customerID' from encoding, as it's an identifier, not a feature
        if 'customerID' in categorical_cols:
            categorical_cols.remove('customerID')
            
        df_encoded = pd.get_dummies(df, columns=categorical_cols, drop_first=True)
        logging.info("Encoded categorical variables using one-hot encoding.")

        # 3. Store the transformed data in SQLite
        db_path = os.path.join(processed_data_dir, 'customer_features.db')
        conn = sqlite3.connect(db_path)
        df_encoded.to_sql('customer_features', conn, if_exists='replace', index=False)
        conn.close()
        logging.info(f"Transformed data saved to SQLite database at {db_path}.")

    except Exception as e:
        logging.error(f"Data transformation failed with error: {e}")

if __name__ == '__main__':
    # Generate filename with current datetime
    log_filename = f"logs/transform_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename=log_filename,  # write logs to file
        filemode='a'             # append mode
    )
    
    transform_data()