#!/usr/bin/env python3
"""
Create test IPC file with datetime and date fields for cross-language testing.
This file is used by Node.js tests to verify datetime conversion from Python Polars.
"""

import polars as pl
from datetime import datetime
import os

# Create test data
dt1 = datetime(2024, 2, 28, 14, 53, 0, 0)
dt2 = datetime(2025, 1, 3, 9, 30, 0, 0)
dt3 = datetime(
    2024, 12, 31, 23, 59, 59, 999000
)  # 999 milliseconds = 999000 microseconds

# Define schema explicitly to ensure Date type is preserved
schema = {
    "id": pl.Int64,
    "datetime_us": pl.Datetime("us"),
    "date_field": pl.Date,
}

df = pl.DataFrame(
    {
        "id": [1, 2, 3],
        "datetime_us": pl.Series(
            "datetime_us", [dt1, dt2, dt3], dtype=pl.Datetime("us")
        ),
        "date_field": pl.Series("date_field", [dt1, dt2, dt3], dtype=pl.Date),
    },
    schema=schema,
)

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
output_path = os.path.join(script_dir, "datasets", "test_datetime_us_python.ipc")

# Write IPC file
df.write_ipc(output_path)
print(f"Created test IPC file: {output_path}")
print(f"DataFrame shape: {df.shape}")
print(f"Schema: {df.schema}")
