import json
from pathlib import Path

import duckdb
import pandas as pd

RAW_PATH = Path("data/raw")


def load_latest_file():
    files = list(RAW_PATH.glob("weather_*.json"))
    if not files:
        raise Exception("No raw files found")

    latest_file = max(files, key=lambda x: x.stat().st_mtime)
    print(f"Loading file: {latest_file}")

    with open(latest_file) as f:
        data = json.load(f)

    return data


def load_to_duckdb(data):
    con = duckdb.connect("weather.duckdb")

    hourly = data.get("hourly", {})

    df = pd.DataFrame({
        "time": hourly.get("time", []),
        "temperature": hourly.get("temperature_2m", []),
        "humidity": hourly.get("relative_humidity_2m", []),
        "wind_speed": hourly.get("wind_speed_10m", []),
    })

    df["time"] = pd.to_datetime(df["time"])

    con.execute("""
        CREATE TABLE IF NOT EXISTS weather_raw (
            time TIMESTAMP,
            temperature DOUBLE,
            humidity DOUBLE,
            wind_speed DOUBLE
        )
    """)

    con.register("weather_df", df)

    con.execute("""
        INSERT INTO weather_raw
        SELECT * FROM weather_df
    """)

    print(f"Loaded {len(df)} rows into DuckDB table: weather_raw")


if __name__ == "__main__":
    data = load_latest_file()
    load_to_duckdb(data)