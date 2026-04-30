import duckdb

con = duckdb.connect("weather.duckdb")

df = con.execute("""
SELECT DISTINCT State, County
FROM epa_aqi_raw
WHERE State IN (
    'Tennessee','Georgia','Illinois','Texas','California',
    'New York','Arizona','Colorado','Washington','Florida','Utah'
)
ORDER BY State, County
LIMIT 100
""").fetchdf()

print(df)