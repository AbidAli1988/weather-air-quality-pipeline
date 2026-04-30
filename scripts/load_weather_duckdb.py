import json
from pathlib import Path

import duckdb
import pandas as pd

RAW_PATH = Path("data/raw")


def load_latest_file():
    files = list(RAW_PATH.glob("weather_multi_*.json"))
    if not files:
        raise Exception("No multi-city weather raw files found")

    latest_file = max(files, key=lambda x: x.stat().st_mtime)
    print(f"Loading file: {latest_file}")

    with open(latest_file) as f:
        return json.load(f)


def transform_weather(data):
    rows = []

    for city_data in data:
        city = city_data["city"]
        state = city_data["state"]
        hourly = city_data.get("hourly", {})

        for i, time_value in enumerate(hourly.get("time", [])):
            rows.append({
                "city": city,
                "state": state,
                "time": time_value,
                "temperature": hourly["temperature_2m"][i],
                "humidity": hourly["relative_humidity_2m"][i],
                "wind_speed": hourly["wind_speed_10m"][i],
            })

    df = pd.DataFrame(rows)
    df["time"] = pd.to_datetime(df["time"])
    return df


def load_to_duckdb(df):
    con = duckdb.connect("weather.duckdb")

    con.execute("DROP TABLE IF EXISTS weather_raw")

    con.execute("""
        CREATE TABLE weather_raw (
            city TEXT,
            state TEXT,
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
    raw_data = load_latest_file()
    weather_df = transform_weather(raw_data)
    load_to_duckdb(weather_df)