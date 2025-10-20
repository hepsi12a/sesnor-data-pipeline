import os
import pandas as pd
import duckdb
from datetime import datetime

RAW_DATA_DIR = "../data/raw"
CHECKPOINT_FILE = "../data/last_ingested.txt"
PROCESSED_DIR = "../data/processed"


def get_last_ingested_date():
    """Reads the last ingested date from checkpoint file."""
    if not os.path.exists(CHECKPOINT_FILE):
        return None
    with open(CHECKPOINT_FILE, "r") as f:
        return f.read().strip()


def update_checkpoint(date_str):
    """Writes the latest ingested date to checkpoint file."""
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(date_str)


def validate_schema(file_path, expected_columns):
    "Validate the schema of the parquet file using DuckDB."
    try:
        con = duckdb.connect()
        result = con.execute(f"DESCRIBE SELECT * FROM parquet_scan('{file_path}')").fetchdf()
        actual_columns = set(result['column_name'].tolist())
        missing = set(expected_columns) - actual_columns
        con.close()
        return len(missing) == 0, missing
    except Exception as e:
        print(f"Schema validation failed for {file_path}: {e}")
        return False, []


def read_parquet_file(file_path):
    """Safely reads a parquet file into a DataFrame."""
    try:
        df = pd.read_parquet(file_path)
        return df
    except Exception as e:
        print(f"Failed to read {file_path}: {e}")
        return None


def log_summary(con, df):
    """Log ingestion summary using DuckDB SQL."""
    con.register("df", df)
    summary = con.execute("""
        SELECT 
            COUNT(*) AS total_records,
            COUNT(DISTINCT sensor_id) AS unique_sensors,
            MIN(timestamp) AS min_time,
            MAX(timestamp) AS max_time
        FROM df
    """).fetchdf()
    print("Ingestion Summary:\n", summary.to_string(index=False))


def ingest_data(raw_dir=RAW_DATA_DIR, processed_dir=PROCESSED_DIR):
    expected_cols = ["sensor_id", "timestamp", "reading_type", "value", "battery_level"]

    last_date = get_last_ingested_date()
    files = sorted(f for f in os.listdir(raw_dir) if f.endswith(".parquet"))

    processed_count = 0
    total_records = 0
    skipped_files = 0

    con = duckdb.connect()

    for file in files:
        date_str = file.replace(".parquet", "")
        if last_date and date_str <= last_date:
            continue

        file_path = os.path.join(raw_dir, file)
        print(f"\n Processing: {file_path}")

        # Validate schema
        valid_schema, missing = validate_schema(file_path, expected_cols)
        if not valid_schema:
            print(f"Skipped {file}: Missing columns {missing}")
            skipped_files += 1
            continue

        # Read data
        df = read_parquet_file(file_path)
        df = df.dropna(subset=["sensor_id", "timestamp", "reading_type", "value"])
        if df.empty:
            skipped_files += 1
            continue

        log_summary(con, df)

        # Save to processed folder
        os.makedirs(processed_dir, exist_ok=True)
        output_file = os.path.join(processed_dir, f"{date_str}_cleaned.parquet")
        df.to_parquet(output_file, index=False)

        processed_count += 1
        total_records += len(df)

        update_checkpoint(date_str)

    con.close()
    print("\n Ingestion Completed!")
    print(f"Files processed: {processed_count}")
    print(f"Records processed: {total_records}")
    print(f"Files skipped: {skipped_files}")



# if __name__ == "__main__":
#     ingest_data()
