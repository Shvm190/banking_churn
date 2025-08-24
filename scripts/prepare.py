# scripts/prepare.py
# Deliverables: A Jupyter notebook/Python script showcasing the process, visualizations, and a clean dataset

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import seaborn as sns
import logging
from datetime import datetime

def prepare_data():
    """
    Cleans and preprocesses the raw data and performs basic EDA.
    """
    logging.info("Starting data preparation...")
    
    try:
        raw_data_dir = os.path.join(os.path.dirname(__file__), '../data/raw')
        prepared_data_dir = os.path.join(os.path.dirname(__file__), '../data/prepared')
        os.makedirs(prepared_data_dir, exist_ok=True)

        # Load datasets
        df_telco = pd.read_csv(os.path.join(raw_data_dir, "telco_churn.csv"))

        # Clean 'TotalCharges' column in Telco data
        df_telco['TotalCharges'] = pd.to_numeric(df_telco['TotalCharges'], errors='coerce')
        df_telco.dropna(subset=['TotalCharges'], inplace=True)
        logging.info("Handled missing values in 'TotalCharges'.")

        # Create a single merged DataFrame (for demonstration purposes)
        df_merged = df_telco
        
        # --- Basic EDA and visualization ---
        plt.style.use('seaborn-v0_8-whitegrid')
        
        # EDA: Churn distribution
        plt.figure(figsize=(6, 4))
        sns.countplot(x='Churn', data=df_telco)
        plt.title('Distribution of Churn')
        plt.savefig(os.path.join(prepared_data_dir, 'churn_distribution.png'))
        plt.close()
        logging.info("Generated churn distribution plot.")
        
        # EDA: Monthly Charges vs Churn
        plt.figure(figsize=(8, 6))
        sns.boxplot(x='Churn', y='MonthlyCharges', data=df_telco)
        plt.title('Monthly Charges by Churn Status')
        plt.savefig(os.path.join(prepared_data_dir, 'monthly_charges_vs_churn.png'))
        plt.close()
        logging.info("Generated monthly charges vs churn plot.")

        # --- Added Charts: Distribution of Churn against other features ---
        
        # Chart 1: Churn vs. Contract Type
        plt.figure(figsize=(8, 6))
        sns.countplot(x='Contract', hue='Churn', data=df_telco)
        plt.title('Churn Rate by Contract Type')
        plt.savefig(os.path.join(prepared_data_dir, 'churn_vs_contract.png'))
        plt.close()
        logging.info("Generated churn vs. contract plot.")
        
        # Chart 2: Churn vs. Internet Service
        plt.figure(figsize=(8, 6))
        sns.countplot(x='InternetService', hue='Churn', data=df_telco)
        plt.title('Churn Rate by Internet Service')
        plt.savefig(os.path.join(prepared_data_dir, 'churn_vs_internet_service.png'))
        plt.close()
        logging.info("Generated churn vs. internet service plot.")
        
        # Chart 3: Churn vs. Tenure (using a distribution plot)
        plt.figure(figsize=(10, 6))
        sns.histplot(data=df_telco, x='tenure', hue='Churn', multiple='stack')
        plt.title('Tenure Distribution by Churn Status')
        plt.savefig(os.path.join(prepared_data_dir, 'tenure_vs_churn.png'))
        plt.close()
        logging.info("Generated tenure vs. churn plot.")
        
        # Save the cleaned dataset
        cleaned_file_path = os.path.join(prepared_data_dir, 'customer_data_cleaned.csv')
        df_merged.to_csv(cleaned_file_path, index=False)
        logging.info(f"Cleaned dataset saved to {cleaned_file_path}.")

    except Exception as e:
        logging.error(f"Data preparation failed with error: {e}")

if __name__ == '__main__':

    # Generate filename with current datetime
    log_filename = f"logs/prepare_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename=log_filename,  # write logs to file
        filemode='a'             # append mode
    )
    

    prepare_data()