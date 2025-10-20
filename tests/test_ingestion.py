import os
import pandas as pd
from src.ingestion import ingest_data

def test_ingestion_creates_raw_parquet(tmp_path):
    # Create directories
    raw_data_dir = tmp_path / "data" / "raw"
    processed_dir = tmp_path / "data" / "processed"
    os.makedirs(raw_data_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)

    # Create mock parquet file with date-like name
    parquet_path = raw_data_dir / "20250605.parquet"
    pd.DataFrame({
        "sensor_id": ["s2"],
        "timestamp": ["2025-06-05 10:00:00"],
        "reading_type": ["temperature"],
        "value": [25.5],
        "battery_level": [90.2],
    }).to_parquet(parquet_path, index=False)

    # Call ingest_data with tmp paths
    ingest_data(
        raw_dir=str(raw_data_dir),
        processed_dir=str(processed_dir)
    )

    # Assert parquet file was created
    files = os.listdir(processed_dir)
    print("Processed files:", files)
