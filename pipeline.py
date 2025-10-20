"""
pipeline.py — Master orchestrator for the data processing pipeline
Flow: Ingestion → Transformation → Validation → Loading
"""

import sys
from src.download_from_drive import download_from_drive_folder
from src.ingestion import ingest_data
from src.transform import clean_and_transform_all_files
from src.validate import run_data_quality_validation
from src.loader import load_and_partition

def run_pipeline():
    try:
        print("Starting Data Pipeline Execution...\n")

        print("Step 1: Download raw files from Drive")
        download_from_drive_folder()

        
        print("Step 2: Ingesting raw data...")
        ingest_data()
        print("Ingestion complete.\n")

        print("Step 3: Transforming data...")
        clean_and_transform_all_files()
        print("Transformation complete.\n")

        print("Step 4: Validating data schema...")
        run_data_quality_validation()
        print("Validation complete.\n")

        print("Step 5: Loading data to target...")
        load_and_partition()
        print("Loading complete.\n")

        print("Pipeline completed successfully!")

    except Exception as e:
        print(f"Pipeline failed due to: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()
