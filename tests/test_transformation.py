import pandas as pd
from src.transform import clean_and_transform_all_files

def test_clean_and_transform(tmp_path):
    # Setup directories
    raw_dir = tmp_path / "raw"
    cleaned_dir = tmp_path / "cleaned"
    transformed_dir = tmp_path / "transformed"

    raw_dir.mkdir(parents=True, exist_ok=True)
    cleaned_dir.mkdir(parents=True, exist_ok=True)
    transformed_dir.mkdir(parents=True, exist_ok=True)

    # Create mock parquet file
    df = pd.DataFrame({
        "sensor_id": ["s1", "s2"],
        "timestamp": ["2025-06-05 10:00:00", "2025-06-05 11:00:00"],
        "reading_type": ["temperature", "humidity"],
        "value": [25.5, 40.2],
        "battery_level": [90, 80]
    })
    df.to_parquet(raw_dir / "20250605.parquet", index=False)

    # Call transformation using tmp_path directories
    clean_and_transform_all_files(
        raw_dir=str(raw_dir),
        cleaned_dir=str(cleaned_dir),
        transformed_dir=str(transformed_dir)
    )

    # Assert files were created
    cleaned_files = list(cleaned_dir.glob("*_cleaned.parquet"))
    transformed_files = list(transformed_dir.glob("*_transformed.parquet"))

    assert cleaned_files, "No cleaned parquet files created"
    assert transformed_files, "No transformed parquet files created"
