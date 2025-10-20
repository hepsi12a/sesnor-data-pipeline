import duckdb
import os
import pandas as pd

TRANSFORMED_DIR = "../data/processed/transformed"
REPORT_PATH = "../data/processed/data_quality_report.csv"

# Expected ranges per reading_type
EXPECTED_RANGES = {
    "temperature": (15, 40),
    "humidity": (10, 90),
}


def run_data_quality_validation():
    print("Running Data Quality Validation using DuckDB...")

    # Connect to DuckDB (in-memory)
    con = duckdb.connect(database=":memory:")

    # Register transformed parquet files
    files = [os.path.join(TRANSFORMED_DIR, f) for f in os.listdir(TRANSFORMED_DIR) if f.endswith("_transformed.parquet")]
    if not files:
        print("No transformed files found for validation.")
    else:
        file_list_str = ", ".join([f"'{f}'" for f in files])

        con.execute(f"""
            CREATE OR REPLACE VIEW transformed_data AS
            SELECT * FROM read_parquet([{file_list_str}]);
        """)

    # Expected types (logical expectation, not exact DuckDB internals)
    EXPECTED_TYPES = {
        "sensor_id_type": "text",
        "timestamp_type": "text",
        "value_type": "real",
        "reading_type_type": "text",
        "battery_type": "real"
    }

    # Map DuckDB internal type names to logical types
    DUCKDB_TO_LOGICAL_TYPE = {
        "VARCHAR": "text",
        "DOUBLE": "real",
        "FLOAT": "real",
        "INTEGER": "integer",
        "BIGINT": "integer"
    }

    schema_check = con.execute("""
        SELECT 
            typeof(sensor_id) AS sensor_id_type,
            typeof(timestamp_iso) AS timestamp_type,
            typeof(value) AS value_type,
            typeof(reading_type) AS reading_type_type,
            typeof(battery_level) AS battery_type
        FROM transformed_data
        LIMIT 1
    """).fetchdf()

    # Validate schema
    mismatches = []
    for col, expected_type in EXPECTED_TYPES.items():
        actual_type = schema_check[col].iloc[0]
        logical_type = DUCKDB_TO_LOGICAL_TYPE.get(actual_type.upper(), actual_type.lower())

        if logical_type != expected_type:
            mismatches.append(f"- {col.replace('_type', '')}: expected {expected_type}, found {actual_type}")

    if mismatches:
        raise ValueError("Schema mismatches found:\n" + "\n".join(mismatches))

    print("✅ Schema validation passed: All column types match expected definitions.")


    # --- Validate expected value ranges ---
    range_violations = con.execute("""
        SELECT reading_type, COUNT(*) AS out_of_range_count
        FROM transformed_data
        WHERE 
            (reading_type = 'temperature' AND (value < 15 OR value > 40)) OR
            (reading_type = 'humidity' AND (value < 10 OR value > 90))
        GROUP BY reading_type
    """).fetchdf()

    # --- Detect hourly data gaps per sensor ---
    hourly_gaps = con.execute("""
        WITH expected AS (
            SELECT
                sensor_id,
                reading_type,
                MIN(timestamp_iso::TIMESTAMP) AS min_t,
                MAX(timestamp_iso::TIMESTAMP) AS max_t
            FROM transformed_data
            GROUP BY sensor_id, reading_type
        ),
        generated AS (
            SELECT
                e.sensor_id,
                e.reading_type,
                g.ts
            FROM expected e,
            generate_series(e.min_t, e.max_t, INTERVAL 1 HOUR) AS g(ts)
        ),
        missing AS (
            SELECT g.sensor_id, g.reading_type, COUNT(*) AS missing_hours
            FROM generated g
            LEFT JOIN transformed_data d
            ON g.sensor_id = d.sensor_id
            AND g.reading_type = d.reading_type
            AND date_trunc('hour', d.timestamp_iso::TIMESTAMP) = g.ts
            WHERE d.sensor_id IS NULL
            GROUP BY g.sensor_id, g.reading_type
        )
        SELECT * FROM missing
        ORDER BY missing_hours DESC
    """).fetchdf()


    # --- Missing values per reading_type ---
    missing_values = con.execute("""
        SELECT reading_type,
            COUNT(*) FILTER (WHERE value IS NULL) * 100.0 / COUNT(*) AS pct_missing_value,
            COUNT(*) FILTER (WHERE battery_level IS NULL) * 100.0 / COUNT(*) AS pct_missing_battery
        FROM transformed_data
        GROUP BY reading_type
    """).fetchdf()

    # --- % of anomalous readings ---
    # --- % of anomalous readings ---
    columns = [c for c in con.execute("DESCRIBE transformed_data").fetchall()]
    column_names = [c[0] for c in columns]

    if "is_outlier" in column_names:
        anomaly_stats = con.execute("""
            SELECT reading_type,
                COUNT(*) FILTER (WHERE is_outlier = TRUE) * 100.0 / COUNT(*) AS pct_anomalous
            FROM transformed_data
            GROUP BY reading_type
        """).fetchdf()
    else:
        print("Column 'is_outlier' not found. Skipping anomaly % calculation.")
        anomaly_stats = pd.DataFrame()


    # --- Time coverage per sensor ---
    time_coverage = con.execute("""
        SELECT sensor_id,
            MIN(timestamp_iso::TIMESTAMP) AS first_record,
            MAX(timestamp_iso::TIMESTAMP) AS last_record,
            (MAX(timestamp_iso::TIMESTAMP) - MIN(timestamp_iso::TIMESTAMP)) AS total_coverage
        FROM transformed_data
        GROUP BY sensor_id
        ORDER BY sensor_id
    """).fetchdf()

    # --- Combine results into a single report ---
    report = {
        "Schema Types": [schema_check.to_dict(orient="records")],
        "Out-of-Range Counts": [range_violations.to_dict(orient="records")],
        "Missing Values %": [missing_values.to_dict(orient="records")],
        "Anomaly %": [anomaly_stats.to_dict(orient="records")],
        "Hourly Gaps": [hourly_gaps.to_dict(orient="records")],
        "Time Coverage": [time_coverage.to_dict(orient="records")],
    }

    report_df = pd.DataFrame.from_dict(report)
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    report_df.to_csv(REPORT_PATH, index=False)

    print(f"Data Quality Report saved at: {REPORT_PATH}")


# if __name__ == "__main__":
#     run_data_quality_validation()
