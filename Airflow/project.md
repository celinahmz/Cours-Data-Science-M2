## Project: Daily Weather Data Pipeline with Airflow


Build a Daily Weather ETL Pipeline Using Open-Meteo and Apache Airflow

⸻

### Objective:
Automate the daily collection of weather data (temperature, wind speed, humidity) for multiple cities. The goal is to ingest the data from a public API every morning, clean and store it, and prepare it for use in dashboards.

You must build a scheduled ETL DAG using Apache Airflow.


⸻

## Data Source:

Use the Open-Meteo API, which requires no authentication and returns weather forecast data.

API Example (Paris):

https://api.open-meteo.com/v1/forecast?latitude=48.85&longitude=2.35&current_weather=true


⸻

## Project Tasks:

1. Extract Task
* Query the Open-Meteo API for 3 cities (e.g., Paris, London, Berlin).
* Parse JSON responses and extract:
* temperature (°C)
* windspeed (km/h)
* weather code

2. Transform Task
* Normalize the JSON structure.
* Add a city field and current timestamp.
* Organize the results in a Pandas DataFrame.

3. Load Task
* Append the data to a CSV file (data/weather_data.csv)
* Ensure idempotency: avoid writing duplicate rows.

⸻

## Scheduling Requirement:
* DAG should run daily at 8 AM UTC
* Use @daily or a CRON expression (0 8 * * *)
* Set catchup=False
