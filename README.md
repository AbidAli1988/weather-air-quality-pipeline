# Weather & Air Quality Data Pipeline

## Overview
This project builds an end-to-end data pipeline that collects weather and air quality data from multiple public APIs, processes it, and loads it into a data warehouse for analysis.

The goal is to understand how weather conditions impact air quality across different U.S. cities.

---

## Tech Stack
- Python (data ingestion)
- AWS S3 (data lake for raw storage)
- DuckDB (local processing)
- dbt (transformations and data modeling)
- Snowflake (data warehouse)
- Apache Airflow (pipeline orchestration)
- Streamlit (dashboard / visualization)

---

## Data Sources
- EPA AirNow API
- Open-Meteo API
- NOAA CDO API
- EPA Historical AQI datasets
- NOAA Daily Weather Summaries

---

## Pipeline Architecture
The pipeline follows a medallion architecture:

- **Bronze (Raw):** Data ingested from APIs and stored in S3
- **Silver (Cleaned):** Data cleaned and standardized
- **Gold (Analytics):** Final tables for reporting and dashboarding

---

## Project Structure