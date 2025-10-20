# src/calibrations.py

CALIBRATION = {
    "temperature": {"multiplier": 1.02, "offset": 0.5, "expected_range": (10, 50)},
    "humidity": {"multiplier": 0.98, "offset": 1.0, "expected_range": (20, 90)},
    "soil_moisture": {"multiplier": 1.0, "offset": 0.0, "expected_range": (0, 100)},
    "light_intensity": {"multiplier": 1.1, "offset": 0.0, "expected_range": (100, 2000)},
}
