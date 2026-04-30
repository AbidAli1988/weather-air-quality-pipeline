import requests
import json
from datetime import datetime
from pathlib import Path
import pandas as pd

RAW_PATH = Path("data/raw")
RAW_PATH.mkdir(parents=True, exist_ok=True)

CITIES_FILE = "config/cities.csv"


def load_cities():
    return pd.read_csv(CITIES_FILE)


def fetch_weather(lat, lon):
    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ["temperature_2m", "relative_humidity_2m", "wind_speed_10m"],
        "timezone": "auto"
    }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        raise Exception(f"Weather API failed: {response.status_code}")

    return response.json()


def main():
    cities = load_cities()

    all_data = []

    for _, row in cities.iterrows():
        print(f"Fetching weather for {row['city']}")

        data = fetch_weather(row["latitude"], row["longitude"])

        data["city"] = row["city"]
        data["state"] = row["state"]

        all_data.append(data)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = RAW_PATH / f"weather_multi_{timestamp}.json"

    with open(file_path, "w") as f:
        json.dump(all_data, f, indent=2)

    print(f"Saved multi-city weather data to {file_path}")


if __name__ == "__main__":
    main()