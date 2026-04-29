import json
from pathlib import Path

import duckdb
import pandas as pd

RAW_PATH = Path("data/raw")


def load_latest_file():
    files = list(RAW_PATH.glob("airnow_*.json"))
    if not files:
        raise Exception("No AirNow raw files found")

    latest_file = max(files, key=lambda x: x.stat().st_mtime)
    print(f"Loading file: {latest_file}")

    with open(latest_file) as f:
        data = json.load(f)

    return data


def load_to_duckdb(data):
    con = duckdb.connect("weather.duckdb")

    df = pd.DataFrame(data)

    con.execute("""
        CREATE TABLE IF NOT EXISTS airnow_raw (
            DateObserved DATE,
            HourObserved INTEGER,
            LocalTimeZone TEXT,
            ReportingArea TEXT,
            StateCode TEXT,
            Latitude DOUBLE,
            Longitude DOUBLE,
            ParameterName TEXT,
            AQI INTEGER,
            Category TEXT
        )
    """)

    con.register("airnow_df", df)

    con.execute("""
        INSERT INTO airnow_raw
        SELECT
            DateObserved,
            HourObserved,
            LocalTimeZone,
            ReportingArea,
            StateCode,
            Latitude,
            Longitude,
            ParameterName,
            AQI,
            Category.Name AS Category
        FROM airnow_df
    """)

    print(f"Loaded {len(df)} rows into DuckDB table: airnow_raw")


if __name__ == "__main__":
    data = load_latest_file()
    load_to_duckdb(data)