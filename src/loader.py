import os
import pandas as pd

# Input folder where transformed files are saved
TRANSFORMED_DIR = "../data/processed/transformed"
# Output folder for optimized Parquet storage
FINAL_OUTPUT_DIR = "../data/processed/final_parquet"


def load_and_partition():
    os.makedirs(FINAL_OUTPUT_DIR, exist_ok=True)

    files = [f for f in os.listdir(TRANSFORMED_DIR) if f.endswith("_transformed.parquet")]
    if not files:
        print("No transformed files found for loading.")
        return

    all_data = []
    for file in files:
        file_path = os.path.join(TRANSFORMED_DIR, file)
        print(f"Loading transformed data: {file_path}")
        try:
            df = pd.read_parquet(file_path)
            if not df.empty:
                all_data.append(df)
        except Exception as e:
            print(f"Failed to read {file_path}: {e}")

    if not all_data:
        print("No data available for final storage.")
        return

    # Combine all transformed data into a single DataFrame
    full_df = pd.concat(all_data, ignore_index=True)
    print(f"Combined {len(full_df)} total records from {len(files)} files.")

    # --- Ensure required columns are present ---
    if "date" not in full_df.columns:
        full_df["date"] = pd.to_datetime(full_df["timestamp"]).dt.date

    if "sensor_id" not in full_df.columns:
        print("Missing sensor_id column, cannot partition by sensor.")
        partition_cols = ["date"]
    else:
        partition_cols = ["date", "sensor_id"]

    # --- Store optimized Parquet ---
    # Using Snappy compression for fast analytics
    output_path = os.path.join(FINAL_OUTPUT_DIR, "agri_sensor_data.parquet")

    print("Writing optimized Parquet dataset...")
    full_df.to_parquet(
        output_path,
        index=False,
        compression="snappy",
        engine="pyarrow"
    )

    print(f"Data stored at: {output_path}")

    # --- Optional: Partitioned save (per date or per sensor) ---
    print("Creating partitioned structure for analytical queries...")
    for (date, sensor_id), group_df in full_df.groupby(partition_cols):
        partition_dir = os.path.join(FINAL_OUTPUT_DIR, f"date={date}", f"sensor_id={sensor_id}")
        os.makedirs(partition_dir, exist_ok=True)
        out_file = os.path.join(partition_dir, f"data_{date}_{sensor_id}.parquet")
        group_df.to_parquet(out_file, index=False, compression="snappy")

    print(f"Partitioned dataset stored under: {FINAL_OUTPUT_DIR}")


# if __name__ == "__main__":
#     load_and_partition()
