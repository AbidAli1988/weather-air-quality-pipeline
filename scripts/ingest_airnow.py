import requests
import json
from datetime import datetime
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

RAW_PATH = Path("data/raw")
RAW_PATH.mkdir(parents=True, exist_ok=True)

API_KEY = os.getenv("AIRNOW_API_KEY")

URL = "https://www.airnowapi.org/aq/observation/latLong/current/"

params = {
    "format": "application/json",
    "latitude": 36.16,
    "longitude": -86.78,
    "distance": 25,
    "API_KEY": API_KEY
}

def fetch_air_quality():
    response = requests.get(URL, params=params)

    if response.status_code != 200:
        raise Exception(f"API failed: {response.status_code}")

    return response.json()


def save_raw(data):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = RAW_PATH / f"airnow_{timestamp}.json"

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Saved AirNow data to {file_path}")


if __name__ == "__main__":
    data = fetch_air_quality()
    save_raw(data)