import json
from pathlib import Path

import duckdb
import pandas as pd

RAW_PATH = Path("data/raw")


def load_latest_file():
    files = list(RAW_PATH.glob("airnow_multi_*.json"))
    if not files:
        raise Exception("No multi-city AirNow files found")

    latest_file = max(files, key=lambda x: x.stat().st_mtime)
    print(f"Loading file: {latest_file}")

    with open(latest_file) as f:
        return json.load(f)


def transform(data):
    df = pd.DataFrame(data)

    # Flatten Category field
    if "Category" in df.columns:
        df["Category"] = df["Category"].apply(lambda x: x.get("Name") if isinstance(x, dict) else x)

    return df


def load_to_duckdb(df):
    con = duckdb.connect("weather.duckdb")

    con.execute("DROP TABLE IF EXISTS airnow_raw")

    con.execute("""
        CREATE TABLE airnow_raw (
            city TEXT,
            state TEXT,
            DateObserved DATE,
            HourObserved INTEGER,
            LocalTimeZone TEXT,
            ReportingArea TEXT,
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
            city,
            state,
            DateObserved,
            HourObserved,
            LocalTimeZone,
            ReportingArea,
            Latitude,
            Longitude,
            ParameterName,
            AQI,
            Category
        FROM airnow_df
    """)

    print(f"Loaded {len(df)} rows into DuckDB table: airnow_raw")


if __name__ == "__main__":
    raw = load_latest_file()
    df = transform(raw)
    load_to_duckdb(df)