
# ---
# scripts/feature_store.py
# Deliverables: Feature store configuration/code and documentation

import json
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
        self.save_metadata()

    def save_metadata(self):
        """Saves the current feature metadata to the JSON file."""
        with open(self.metadata_path, 'w') as f:
            json.dump(self.features, f, indent=4)
        logging.info("Feature metadata saved.")

# Example Usage to add features
if __name__ == '__main__':
    metadata_file = os.path.join(os.path.dirname(__file__), '../features.json')
    fs = FeatureStore(metadata_file)
    
    # Add metadata for the new features created in the transform step
    fs.add_feature_metadata(
        "avg_monthly_charge_per_tenure",
        "Average monthly charge divided by customer tenure in months. A high value may indicate a new, high-value customer.",
        "derived from MonthlyCharges and tenure",
        "1.0"
    )
    
    fs.add_feature_metadata(
        "IsSeniorCitizen",
        "A binary feature indicating if the customer is 65 years or older. This is derived from the 'age' column.",
        "derived from age",
        "1.0"
    )

    logging.info("Feature store demonstration complete. Check your `features.json` file for the output.")
