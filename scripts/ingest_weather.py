import requests
import json
from datetime import datetime
from pathlib import Path

# Create raw data folder if not exists
RAW_PATH = Path("data/raw")
RAW_PATH.mkdir(parents=True, exist_ok=True)

# Open-Meteo API endpoint
URL = "https://api.open-meteo.com/v1/forecast"

params = {
    "latitude": 36.16,   # Nashville
    "longitude": -86.78,
    "hourly": ["temperature_2m", "relative_humidity_2m", "wind_speed_10m"],
    "timezone": "America/Chicago"
}

def fetch_weather_data():
    response = requests.get(URL, params=params)

    if response.status_code != 200:
        raise Exception(f"API request failed: {response.status_code}")

    return response.json()


def save_raw_data(data):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = RAW_PATH / f"weather_{timestamp}.json"

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Saved raw data to {file_path}")


if __name__ == "__main__":
    data = fetch_weather_data()
    save_raw_data(data)