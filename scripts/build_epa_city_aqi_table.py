import duckdb

con = duckdb.connect("weather.duckdb")

query = """
CREATE OR REPLACE TABLE epa_city_aqi AS
WITH cities AS (
    SELECT *
    FROM read_csv_auto('config/cities.csv', header=True)
),
state_map AS (
    SELECT * FROM (
        VALUES
        ('TN', 'Tennessee'),
        ('GA', 'Georgia'),
        ('IL', 'Illinois'),
        ('TX', 'Texas'),
        ('CA', 'California'),
        ('NY', 'New York'),
        ('AZ', 'Arizona'),
        ('CO', 'Colorado'),
        ('WA', 'Washington'),
        ('FL', 'Florida'),
        ('UT', 'Utah')
    ) AS t(state_code, state_name)
)
SELECT
    c.city,
    c.state,
    c.county,
    e.Year AS year,
    e."Median AQI" AS median_aqi,
    e."Max AQI" AS max_aqi,
    e."Good Days" AS good_days,
    e."Moderate Days" AS moderate_days,
    e."Unhealthy Days" AS unhealthy_days
FROM epa_aqi_raw e
JOIN cities c
    ON lower(replace(e.County, '-', '')) = lower(replace(c.county, '-', ''))
JOIN state_map sm
    ON c.state = sm.state_code
    AND lower(e.State) = lower(sm.state_name)
"""

con.execute(query)

print("Created table: epa_city_aqi")