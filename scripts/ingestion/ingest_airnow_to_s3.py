import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

RAW_PATH = Path("data/raw")
RAW_PATH.mkdir(parents=True, exist_ok=True)

CITIES_FILE = "config/cities.csv"
API_KEY = os.getenv("AIRNOW_API_KEY")


def load_cities():
    return pd.read_csv(CITIES_FILE)


def fetch_airnow(lat, lon):
    url = "https://www.airnowapi.org/aq/observation/latLong/current/"

    params = {
        "format": "application/json",
        "latitude": lat,
        "longitude": lon,
        "distance": 25,
        "API_KEY": API_KEY,
    }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        raise Exception(f"AirNow API failed: {response.status_code} - {response.text}")

    return response.json()


def main():
    if not API_KEY:
        raise Exception("Missing AIRNOW_API_KEY in .env")

    cities = load_cities()
    all_data = []

    for _, row in cities.iterrows():
        print(f"Fetching AirNow data for {row['city']}")

        data = fetch_airnow(row["latitude"], row["longitude"])

        for record in data:
            record["city"] = row["city"]
            record["state"] = row["state"]

        all_data.extend(data)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = RAW_PATH / f"airnow_multi_{timestamp}.json"

    with open(file_path, "w") as f:
        json.dump(all_data, f, indent=2)

    print(f"Saved multi-city AirNow data to {file_path}")
    print(f"Total records: {len(all_data)}")


if __name__ == "__main__":
    main()