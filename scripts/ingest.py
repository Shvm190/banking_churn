# scripts/ingest.py
# Deliverables: Python scripts for ingestion and screenshots of ingested data

import pandas as pd
import os
import logging
from datetime import datetime
import kagglehub

# We will use the Hugging Face `datasets` library to load the data
from datasets import load_dataset

def ingest_data():
    """
    Ingests data from two different sources and saves them in the raw data directory.
    Fetches the Telco Customer Churn dataset and a Hugging Face banking dataset.
    """
    try:
        # Create directories if they don't exist
        raw_data_dir = os.path.join(os.path.dirname(__file__), '../data/raw')
        os.makedirs(raw_data_dir, exist_ok=True)

        # Ingest Telco Customer Churn data from Kaggle
        logging.info("Starting ingestion of Telco Customer Churn data from Kaggle...")
        # Use kagglehub to download the dataset
        # The downloaded path is a directory, we need to find the CSV file within it
        path = kagglehub.dataset_download("blastchar/telco-customer-churn")
        telco_file_name = "WA_Fn-UseC_-Telco-Customer-Churn.csv"
        df_telco = pd.read_csv(os.path.join(path, telco_file_name))
        
        telco_file_path = os.path.join(raw_data_dir, "telco_churn.csv")
        df_telco.to_csv(telco_file_path, index=False)
        logging.info(f"Successfully ingested Telco data. Rows: {len(df_telco)}, Columns: {len(df_telco.columns)}")

        # Ingest Bank Customer Support data from Hugging Face
        logging.info("Starting ingestion of Bank Customer Support data from Hugging Face...")
        
        # Load the dataset from Hugging Face and convert it to a pandas DataFrame
        dataset_hf = load_dataset('suDEEP101/bank_customer_support', split='train')
        df_hf = dataset_hf.to_pandas()
        
        hf_file_path = os.path.join(raw_data_dir, "hf_bank_customer_support.csv")
        df_hf.to_csv(hf_file_path, index=False)
        logging.info(f"Successfully ingested Hugging Face data. Rows: {len(df_hf)}, Columns: {len(df_hf.columns)}")

    except Exception as e:
        logging.error(f"Data ingestion failed with error: {e}")
    
if __name__ == '__main__':
    # Generate filename with current datetime
    log_filename = f"logs/ingest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename=log_filename,  # write logs to file
        filemode='a'             # append mode
    )
    
    ingest_data()
# curl -X GET \
#      "https://datasets-server.huggingface.co/splits?dataset=aai510-group1%2Ftelco-customer-churn"