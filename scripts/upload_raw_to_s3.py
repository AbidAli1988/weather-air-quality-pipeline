import os
from pathlib import Path

import boto3
from dotenv import load_dotenv

load_dotenv()

AWS_PROFILE = os.getenv("AWS_PROFILE")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX")

RAW_PATH = Path("data/raw")


def get_s3_client():
    session = boto3.Session(
        profile_name=AWS_PROFILE,
        region_name=AWS_REGION,
    )
    return session.client("s3")


def upload_file(s3_client, local_file: Path):
    relative_path = local_file.relative_to(RAW_PATH).as_posix()
    s3_key = f"{S3_PREFIX}/raw/{relative_path}"

    print(f"Uploading {local_file} → s3://{S3_BUCKET}/{s3_key}")

    s3_client.upload_file(
        Filename=str(local_file),
        Bucket=S3_BUCKET,
        Key=s3_key,
    )


def main():
    if not all([AWS_PROFILE, AWS_REGION, S3_BUCKET, S3_PREFIX]):
        raise Exception("Missing AWS/S3 values in .env")

    if not RAW_PATH.exists():
        raise Exception("data/raw folder not found")

    s3_client = get_s3_client()

    files = [file for file in RAW_PATH.rglob("*") if file.is_file()]

    if not files:
        print("No raw files found to upload.")
        return

    for file in files:
        upload_file(s3_client, file)

    print(f"Uploaded {len(files)} raw files to S3.")


if __name__ == "__main__":
    main()