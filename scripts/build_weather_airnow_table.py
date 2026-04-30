import duckdb

con = duckdb.connect("weather.duckdb")

query = """
CREATE OR REPLACE TABLE weather_air_quality AS
SELECT
    w.city,
    w.state,
    w.time,
    w.temperature,
    w.humidity,
    w.wind_speed,
    a.ParameterName,
    a.AQI,
    a.Category
FROM weather_raw w
JOIN airnow_raw a
    ON w.city = a.city
    AND w.state = a.state
    AND DATE(w.time) = a.DateObserved
    AND EXTRACT(HOUR FROM w.time) = a.HourObserved
"""

con.execute(query)

print("Created multi-city table: weather_air_quality")