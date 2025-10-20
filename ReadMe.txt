Data Pipeline Execution Guide

This project contains a data pipeline with ingestion,transformation, validation and loader modules. 
This README explains how to execute each component and their tests.

1. Project Structure
project_root/
├─ src/
│  ├─ ingestion.py       # Data ingestion script
│  ├─ transform.py       # Data transform script
│  ├─ validate.py        # Data validation script
│  ├─ loader.py          # Data loader & partitioner
├─ tests/
│  ├─ test_ingestion.py         # Pytest for ingestion
│  ├─ test_transformation.py    # Pytest for transformation
│  ├─ test_validation.py        # Pytest for validation
│  ├─ test_loader.py            # Pytest for loader
├─ pipeline.py
├─ Dockerfile
├─ README.md

2. Setup

Python 3.9+ is installed.

Install required packages:

pip install -r requirements.txt

3. Run the pipeline from pipeline.py 

which has the processing flow of ingestion, transformation, validation and loading

Purpose:

Check schema consistency of transformed parquet files.

Validate value ranges and missing data.

Detect hourly gaps in sensor data.

Generate a data quality report.

Run validation manually:

Input is from google drive - https://drive.google.com/drive/folders/1-ybcSvjPf6pYHIMzBj-hiAhKPIxn6Isn?usp=sharing

Output will be saved to ../data/processed/data_quality_report.csv (configurable inside validate.py)

3.1 Run Validation Tests

Test file: tests/test_validation.py

Command to run all test cases:

pytest -v


Checks performed by test:

Schema validation passes for transformed Parquet files.

Data quality report is generated correctly.


COMMANDS TO RUN:

docker build --no-cache -t data-pipeline:v2 .
docker run -it data-pipeline:v2

command to run test cases:
 pytest -v


