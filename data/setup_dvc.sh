
cd banking_churn
git init

dvc init

git add .
git commit -m "Initialize project with Git and DVC"

dvc remote add -d local_data_store ../dvc-data-store

git add .dvc/config
git commit -m "Configure DVC remote storage"


# Track Files with DVC and Git
# This is a placeholder for your script's output
mkdir -p data/raw data/prepared data/processed data/validation_report models
touch data/raw/telco_churn.csv data/raw/hf_bank_customer_support.csv
touch data/validation_report.csv
touch data/prepared/customer_data_cleaned.csv
touch data/processed/customer_features.db
# touch models/model.pkl


# Add files to DVC:
dvc add data/raw/telco_churn.csv
dvc add data/raw/hf_bank_customer_support.csv
dvc add data/validation_report.csv
dvc add data/prepared/customer_data_cleaned.csv
dvc add data/processed/customer_features.db
# dvc add models/model.pkl


# Add the .dvc files to Git:
git add data/raw/telco_churn.csv.dvc
git add data/raw/hf_bank_customer_support.csv.dvc
git add data/validation_report.csv.dvc
git add data/prepared/customer_data_cleaned.csv.dvc
git add data/processed/customer_features.db.dvc
# git add models/model.pkl.dvc


# Commit the changes:
git add .
git commit -m "Add and version data files with DVC"


# Push the data to the DVC remote:
dvc push

git remote add origin https://github.com/Shvm190/banking_churn.git

git push -u origin main

