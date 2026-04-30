import duckdb
from pathlib import Path

DATA_PATH = Path("data/raw/epa_aqi")

# Find CSV file
files = list(DATA_PATH.glob("annual_aqi_by_county_*.csv"))

if not files:
    raise Exception("EPA AQI CSV not found")

file_path = files[0]
print(f"Loading: {file_path}")

con = duckdb.connect("weather.duckdb")

# Load directly from CSV (no pandas)
con.execute(f"""
CREATE OR REPLACE TABLE epa_aqi_raw AS
SELECT *
FROM read_csv_auto('{file_path}', header=True)
""")

print("Loaded EPA AQI dataset into table: epa_aqi_raw")