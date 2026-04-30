import os

import boto3
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

AWS_PROFILE = os.getenv("AWS_PROFILE")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX")

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")


def get_aws_temp_credentials():
    session = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)
    credentials = session.get_credentials().get_frozen_credentials()

    return credentials.access_key, credentials.secret_key, credentials.token


def get_snowflake_connection():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema="RAW",
    )


def main():
    aws_key, aws_secret, aws_token = get_aws_temp_credentials()

    s3_url = f"s3://{S3_BUCKET}/{S3_PREFIX}/raw/"

    conn = get_snowflake_connection()
    cur = conn.cursor()

    cur.execute("CREATE SCHEMA IF NOT EXISTS RAW")

    cur.execute("""
        CREATE OR REPLACE FILE FORMAT RAW.JSON_FORMAT
        TYPE = JSON
        STRIP_OUTER_ARRAY = TRUE
    """)

    cur.execute("""
        CREATE OR REPLACE FILE FORMAT RAW.CSV_FORMAT
        TYPE = CSV
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1
    """)

    cur.execute(f"""
        CREATE OR REPLACE STAGE RAW.S3_RAW_STAGE
        URL = '{s3_url}'
        CREDENTIALS = (
            AWS_KEY_ID = '{aws_key}'
            AWS_SECRET_KEY = '{aws_secret}'
            AWS_TOKEN = '{aws_token}'
        )
    """)

    print(f"Created Snowflake stage for: {s3_url}")

    rows = cur.execute("LIST @RAW.S3_RAW_STAGE").fetchall()

    print("Files Snowflake can see:")
    for row in rows:
        print(row[0])

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()