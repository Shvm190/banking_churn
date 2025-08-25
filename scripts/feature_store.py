# scripts/feature_store.py
# Deliverables: Auto-generate feature metadata for all engineered/transformed features

import json
import logging
import os
import sqlite3
from datetime import datetime

class FeatureStore:
    """
    A simple, custom solution to simulate a feature store.
    It manages features and their metadata from a JSON file.
    """
    def __init__(self, metadata_path):
        self.metadata_path = metadata_path
        self.features = self.load_metadata()

    def load_metadata(self):
        """Loads feature metadata from a JSON file."""
        if os.path.exists(self.metadata_path):
            with open(self.metadata_path, 'r') as f:
                return json.load(f)
        else:
            logging.warning("Feature metadata file not found. Initializing empty store.")
            return {}

    def get_feature_metadata(self, feature_name):
        """Retrieves metadata for a specific feature."""
        return self.features.get(feature_name)

    def add_feature_metadata(self, feature_name, description, source, version):
        """Adds or updates metadata for a feature."""
        self.features[feature_name] = {
            "description": description,
            "source": source,
            "version": version
        }
        logging.info(f"Added metadata for feature: {feature_name}")

    def save_metadata(self):
        """Saves the current feature metadata to the JSON file."""
        with open(self.metadata_path, 'w') as f:
            json.dump(self.features, f, indent=4)
        logging.info("Feature metadata saved.")


def auto_register_features(db_path, metadata_path):
    """
    Auto-discovers features from the transformed SQLite DB and registers them in the Feature Store.
    """
    fs = FeatureStore(metadata_path)

    # Connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get column names from 'customer_features' table
    cursor.execute("PRAGMA table_info(customer_features);")
    columns = cursor.fetchall()
    conn.close()

    for col in columns:
        feature_name = col[1]

        # Skip ID column if present
        if feature_name.lower() in ["customerid"]:
            continue

        # Auto-generate description and source
        if feature_name == "avg_monthly_charge_per_tenure":
            desc = "Average monthly charge divided by customer tenure in months."
            src = "Derived from MonthlyCharges and tenure"
        elif feature_name.startswith("Contract_"):
            desc = f"Binary feature indicating if customer contract type is {feature_name.replace('Contract_', '')}."
            src = "Derived from Contract column"
        elif feature_name.startswith("InternetService_"):
            desc = f"Binary feature indicating if internet service type is {feature_name.replace('InternetService_', '')}."
            src = "Derived from InternetService column"
        elif feature_name.startswith("PaymentMethod_"):
            desc = f"Binary feature indicating if payment method is {feature_name.replace('PaymentMethod_', '')}."
            src = "Derived from PaymentMethod column"
        else:
            desc = f"Feature derived from original dataset column: {feature_name}."
            src = "Telco churn dataset"

        fs.add_feature_metadata(
            feature_name,
            desc,
            src,
            "1.0"
        )

    fs.save_metadata()


if __name__ == '__main__':

    # Generate filename with current datetime
    log_filename = f"logs/featureStore_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename=log_filename,  # write logs to file
        filemode='a'             # append mode
    )
    
    metadata_file = os.path.join(os.path.dirname(__file__), '../features.json')
    db_path = os.path.join(os.path.dirname(__file__), '../data/processed/customer_features.db')

    auto_register_features(db_path, metadata_file)
    logging.info("Auto feature registration complete. Check `features.json` for output.")
