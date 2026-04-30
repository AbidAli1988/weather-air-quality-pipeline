import json
import os
from datetime import datetime, timezone

import boto3
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

AWS_PROFILE = os.getenv("AWS_PROFILE")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX")

CITIES_FILE = "config/cities.csv"


def get_s3_client():
    session = boto3.Session(
        profile_name=AWS_PROFILE,
        region_name=AWS_REGION,
    )
    return session.client("s3")


def load_cities():
    return pd.read_csv(CITIES_FILE)


def fetch_weather(lat, lon):
    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ["temperature_2m", "relative_humidity_2m", "wind_speed_10m"],
        "timezone": "auto",
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    return response.json()


def upload_json_to_s3(s3_client, data, s3_key):
    body = json.dumps(data, indent=2)

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=body,
        ContentType="application/json",
    )

    print(f"Uploaded to s3://{S3_BUCKET}/{s3_key}")


def main():
    required_env = [AWS_PROFILE, AWS_REGION, S3_BUCKET, S3_PREFIX]
    if not all(required_env):
        raise Exception("Missing AWS/S3 values in .env")

    cities = load_cities()
    s3_client = get_s3_client()

    all_weather_data = []

    for _, row in cities.iterrows():
        city = row["city"]
        state = row["state"]

        print(f"Fetching weather for {city}, {state}")

        data = fetch_weather(row["latitude"], row["longitude"])

        data["city"] = city
        data["state"] = state
        data["county"] = row["county"]
        data["ingested_at_utc"] = datetime.now(timezone.utc).isoformat()

        all_weather_data.append(data)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    s3_key = f"{S3_PREFIX}/raw/weather/open_meteo/weather_multi_{timestamp}.json"

    upload_json_to_s3(
        s3_client=s3_client,
        data=all_weather_data,
        s3_key=s3_key,
    )

    print(f"Uploaded weather records for {len(all_weather_data)} cities.")


if __name__ == "__main__":
    main()