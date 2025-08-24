# scripts/validate_data.py

import pandas as pd
import numpy as np
import os
import logging

from datetime import datetime


def generate_validation_report(df: pd.DataFrame) -> dict:
    """
    Generates a data validation report for the Telco Churn dataset, including examples.

    Args:
        df: The raw pandas DataFrame.

    Returns:
        A dictionary containing the validation report.
    """
    report = {}
    
    # Check for duplicates
    report['duplicates'] = df.duplicated().sum()
    
    # Check for missing values
    report['missing_values'] = df.isnull().sum().to_dict()
    
    # Check for inconsistent categorical values (e.g., blank spaces)
    df_cleaned = df.replace(' ', np.nan)
    report['inconsistent_values'] = df_cleaned.isnull().sum().to_dict()
    
    # Check data types
    report['inferred_dtypes'] = df.dtypes.astype(str).to_dict()

    # --- ADDED: Sample of inconsistent and consistent values for TotalCharges ---
    report['value_examples'] = {}
    
    # Find inconsistent values in TotalCharges (blanks)
    inconsistent_total_charges = df[df['TotalCharges'] == ' ']
    
    # Get up to 5 examples of inconsistent values
    inconsistent_examples = inconsistent_total_charges['TotalCharges'].sample(min(5, len(inconsistent_total_charges))).tolist()
    report['value_examples']['inconsistent_total_charges'] = inconsistent_examples
    
    # Find consistent (numeric) values in TotalCharges
    consistent_total_charges = df[pd.to_numeric(df['TotalCharges'], errors='coerce').notna()]
    
    # Get up to 5 examples of consistent values
    consistent_examples = consistent_total_charges['TotalCharges'].sample(min(5, len(consistent_total_charges))).tolist()
    report['value_examples']['consistent_total_charges'] = consistent_examples
    
    return report

if __name__ == '__main__':

    # Generate filename with current datetime
    log_filename = f"logs/validate_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename=log_filename,  # write logs to file
        filemode='a'             # append mode
    )
    

    try:
        logging.info("Starting data validation...")
        
        # Load the raw dataset
        raw_data_path = os.path.join(os.path.dirname(__file__), '../data/raw/telco_churn.csv')
        df_telco = pd.read_csv(raw_data_path)
        
        validation_report = generate_validation_report(df_telco)
        
        # --- FIX HERE: Reformat the dictionary to prevent the error ---
        
        # Create a list to hold the flattened report data
        report_data_list = []
        for key, value in validation_report.items():
            if isinstance(value, dict):
                # If the value is a dictionary (like for examples), add each key-value pair as a new row
                for sub_key, sub_value in value.items():
                    report_data_list.append({'Metric': f"{key} - {sub_key}", 'Value': str(sub_value)})
            else:
                # Otherwise, add the metric and its value as a single row
                report_data_list.append({'Metric': key, 'Value': str(value)})
        
        # Create the DataFrame from the flattened list
        report_df = pd.DataFrame(report_data_list)
        
        # Save the report to a CSV file
        report_path = os.path.join(os.path.dirname(__file__), '../data/validation_report.csv')
        report_df.to_csv(report_path, index=False)
        
        logging.info(f"Validation complete. Report saved to {report_path}")
        
    except FileNotFoundError:
        logging.error("The 'telco_churn.csv' file was not found.")
    except Exception as e:
        logging.error(f"Data validation failed with error: {e}")