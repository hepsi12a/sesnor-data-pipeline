import duckdb
import pandas as pd
from src.validate import run_data_quality_validation

def test_validation_schema_pass(tmp_path, monkeypatch):
    transformed_dir = tmp_path / "data" / "processed" / "transformed"
    transformed_dir.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame({
        "sensor_id": ["s1"],
        "timestamp_iso": ["2025-06-05T12:00:00Z"],
        "value": [28.5],
        "reading_type": ["temperature"],
        "battery_level": [85.0]
    })
    df.to_parquet(transformed_dir / "sensor_transformed.parquet")

    monkeypatch.setattr("src.validate.TRANSFORMED_DIR", str(transformed_dir))
    monkeypatch.setattr("src.validate.REPORT_PATH", str(tmp_path / "report.csv"))

    run_data_quality_validation()

    assert (tmp_path / "report.csv").exists(), "Data quality report not generated"
