import os
import pandas as pd
import numpy as np
from datetime import timedelta


RAW_PROCESSED_DIR = "../data/raw"
CLEANED_OUTPUT_DIR = "../data/processed/cleaned_only"
TRANSFORMED_OUTPUT_DIR = "../data/processed/transformed"

# --- Calibration parameters ---
CALIBRATION = {
    "temperature": {"multiplier": 1.02, "offset": -0.5},
    "humidity": {"multiplier": 0.98, "offset": 0.3},
}

# --- Expected ranges for anomaly detection ---
EXPECTED_RANGES = {
    "temperature": (15, 40),
    "humidity": (10, 90),
}


def compute_zscore(x):
    "Compute absolute z-score manually."
    mean = x.mean()
    std = x.std(ddof=0)
    if std == 0 or np.isnan(std):
        return np.zeros_like(x)
    return np.abs((x - mean) / std)


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    "Clean data: remove duplicates, handle missing values, and correct outliers."
    df = df.drop_duplicates()

    # Drop rows with missing critical columns
    critical_cols = ["sensor_id", "timestamp", "reading_type", "value"]
    df = df.dropna(subset=critical_cols)

    # Compute z-score and correct outliers
    df["zscore"] = df.groupby("reading_type")["value"].transform(compute_zscore)
    df["is_outlier"] = df["zscore"] > 3
    df.loc[df["is_outlier"], "value"] = df.groupby("reading_type")["value"].transform("median")

    df = df.drop(columns=["zscore"])
    return df


def transform_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    "Add derived and normalized fields."
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Daily average per sensor and reading_type
    df["date"] = df["timestamp"].dt.date
    daily_avg = (
        df.groupby(["date", "sensor_id", "reading_type"])["value"]
        .mean()
        .reset_index(name="daily_avg_value")
    )
    df = df.merge(daily_avg, on=["date", "sensor_id", "reading_type"], how="left")

    # 7-day rolling average per sensor
    df = df.sort_values(by=["sensor_id", "timestamp"])
    df["7d_rolling_avg"] = (
        df.groupby(["sensor_id", "reading_type"])["value"]
        .transform(lambda x: x.rolling(window=7, min_periods=1).mean())
    )

    # Apply calibration normalization
    def normalize(row):
        params = CALIBRATION.get(row["reading_type"], {"multiplier": 1, "offset": 0})
        return row["value"] * params["multiplier"] + params["offset"]

    df["normalized_value"] = df.apply(normalize, axis=1)

    # Convert timestamps to UTC+5:30 and ISO 8601
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["timestamp_iso"] = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    return df


def clean_and_transform_all_files(raw_dir="../data/raw",
                                  cleaned_dir="../data/processed/cleaned_only",
                                  transformed_dir="../data/processed/transformed"):
    """
    Clean and transform all files in raw_dir and save to cleaned_dir / transformed_dir.
    """
    os.makedirs(cleaned_dir, exist_ok=True)
    os.makedirs(transformed_dir, exist_ok=True)

    files = [f for f in os.listdir(raw_dir) if f.endswith(".parquet")]

    for file in files:
        file_path = os.path.join(raw_dir, file)
        print(f"\n Cleaning + Transforming file: {file_path}")

        try:
            df = pd.read_parquet(file_path)
            if df.empty:
                print("Empty file, skipping.")
                continue

            cleaned_df = clean_dataframe(df)
            cleaned_path = os.path.join(cleaned_dir, file.replace(".parquet", "_cleaned.parquet"))
            cleaned_df.to_parquet(cleaned_path, index=False)
            print(f"Cleaned file saved: {cleaned_path}")

            transformed_df = transform_dataframe(cleaned_df)
        
            transformed_path = os.path.join(transformed_dir, file.replace(".parquet", "_transformed.parquet"))
            transformed_df.to_parquet(transformed_path, index=False)
            print(f"Transformed file saved: {transformed_path}")

            print(f"Records processed: {len(df)}, after cleaning: {len(cleaned_df)}")

        except Exception as e:
            print(f"Failed to process {file}: {e}")


# if __name__ == "__main__":
#     clean_and_transform_all_files()
