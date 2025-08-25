from prefect import flow, task
import subprocess
import os
import sys

PYTHON_EXECUTABLE = "/Users/homeaccount/miniconda3/envs/prefect_env/bin/python"
PIPELINE_DIR = os.path.dirname(os.path.abspath(__file__))
DVC_EXECUTABLE = "/Users/homeaccount/miniconda3/envs/prefect_env/bin/dvc"
sys.path.append('/Users/homeaccount/miniconda3/envs/prefect_env/bin/')


def dvc_add_safe(*files, commit_message=None):
    """Add files to DVC only if there are changes."""
    try:
        # Check if DVC thinks the files are changed
        # Add capture_output=True and text=True to get stdout and stderr
        result = subprocess.run([DVC_EXECUTABLE, "status", "--unchanged"] + list(files), capture_output=True, text=True, cwd=PIPELINE_DIR)
        
        # DVC status returns a non-zero exit code if files are unchanged.
        # It's better to check the return code instead of the stdout.
        # This will now work as intended.
        if result.returncode != 0:
            print(f"Changes detected for {files}. Adding to DVC...")
            subprocess.run([DVC_EXECUTABLE, "add"] + list(files), check=True, cwd=PIPELINE_DIR)
            subprocess.run(["git", "add"] + [f"{f}.dvc" for f in files], check=True, cwd=PIPELINE_DIR)
            if commit_message:
                subprocess.run(["git", "commit", "-m", commit_message], check=True, cwd=PIPELINE_DIR)
            subprocess.run([DVC_EXECUTABLE, "push"], check=True, cwd=PIPELINE_DIR)
        else:
            print(f"No changes detected for {files}, skipping DVC add/push.")
            
    except subprocess.CalledProcessError as e:
        print(f"Skipping DVC add for {files}, caught error: {e}")

@task
def run_ingestion():
    subprocess.run([PYTHON_EXECUTABLE, "scripts/ingest.py"], check=True, cwd=PIPELINE_DIR)
    dvc_add_safe("data/raw/telco_churn.csv", "data/raw/hf_bank_customer_support.csv",
                 commit_message="Version raw ingested data")

@task
def run_validation():
    subprocess.run([PYTHON_EXECUTABLE, "scripts/validate.py"], check=True, cwd=PIPELINE_DIR)
    dvc_add_safe("data/validation_report.csv", commit_message="Version validation report")

@task
def run_prepare():
    subprocess.run([PYTHON_EXECUTABLE, "scripts/prepare.py"], check=True, cwd=PIPELINE_DIR)
    dvc_add_safe("data/prepared/customer_data_cleaned.csv", commit_message="Version prepared data")

@task
def run_transform():
    subprocess.run([PYTHON_EXECUTABLE, "scripts/transform.py"], check=True, cwd=PIPELINE_DIR)
    dvc_add_safe("data/processed/customer_features.db", commit_message="Version transformed features")

@task
def run_feature_store():
    subprocess.run([PYTHON_EXECUTABLE, "scripts/feature_store.py"], check=True, cwd=PIPELINE_DIR)
    dvc_add_safe("features.json", commit_message="Version feature store metadata")

@task
def run_training():
    subprocess.run([PYTHON_EXECUTABLE, "scripts/model_training.py", "--db-path", "data/processed/customer_features.db"], check=True, cwd=PIPELINE_DIR)
    dvc_add_safe("models/model.pkl", commit_message="Version trained model")

@flow
def churn_pipeline():
    run_ingestion()
    run_validation()
    run_prepare()
    run_transform()
    run_feature_store()
    run_training()

if __name__ == "__main__":
    churn_pipeline()