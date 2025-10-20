import os
import pandas as pd
import pytest
from pathlib import Path
from src.loader import load_and_partition, FINAL_OUTPUT_DIR, TRANSFORMED_DIR

@pytest.fixture
def setup_transformed_data(tmp_path):
    # Create fake transformed directory
    transformed_dir = tmp_path / "data" / "processed" / "transformed"
    transformed_dir.mkdir(parents=True, exist_ok=True)

    # Create a small example DataFrame
    df = pd.DataFrame({
        "sensor_id": ["s1", "s2"],
        "timestamp": ["2025-06-05T12:00:00Z", "2025-06-05T13:00:00Z"],
        "timestamp_iso": ["2025-06-05T12:00:00+0530", "2025-06-05T13:00:00+0530"],
        "value": [28.5, 30.0],
        "reading_type": ["temperature", "humidity"],
        "battery_level": [85.0, 90.0],
        "date": ["2025-06-05", "2025-06-05"]
    })

    # Save as transformed parquet
    file_path = transformed_dir / "sensor_transformed.parquet"
    df.to_parquet(file_path, index=False)

    return tmp_path, transformed_dir

def test_loader_creates_final_and_partitioned_files(monkeypatch, setup_transformed_data):
    tmp_path, transformed_dir = setup_transformed_data

    # Monkeypatch the directories in loader.py
    monkeypatch.setattr("src.loader.TRANSFORMED_DIR", str(transformed_dir))
    final_dir = tmp_path / "data" / "processed" / "final_parquet"
    monkeypatch.setattr("src.loader.FINAL_OUTPUT_DIR", str(final_dir))

    # Run loader
    load_and_partition()

    # Check final combined Parquet
    final_file = final_dir / "agri_sensor_data.parquet"
    assert final_file.exists(), "Final combined Parquet file not created"

    # Check partitioned directories/files
    partition_dir_s1 = final_dir / "date=2025-06-05" / "sensor_id=s1"
    partition_dir_s2 = final_dir / "date=2025-06-05" / "sensor_id=s2"

    assert partition_dir_s1.exists(), "Partition directory for sensor s1 not created"
    assert partition_dir_s2.exists(), "Partition directory for sensor s2 not created"

    # Each partition should have at least one parquet file
    files_s1 = list(partition_dir_s1.glob("*.parquet"))
    files_s2 = list(partition_dir_s2.glob("*.parquet"))
    assert len(files_s1) > 0, "No parquet file in sensor s1 partition"
    assert len(files_s2) > 0, "No parquet file in sensor s2 partition"

    # Optional: read one partition file to check content
    df_s1 = pd.read_parquet(files_s1[0])
    assert "sensor_id" in df_s1.columns, "Partitioned file missing 'sensor_id' column"
    assert df_s1["sensor_id"].iloc[0] == "s1"
