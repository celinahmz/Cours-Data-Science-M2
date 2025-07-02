from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pytz import timezone
import pandas as pd
import requests
import os
from dateutil import parser

# -------------------------------
# Liste des villes à interroger
# -------------------------------
cities = {
    "Paris": {"lat": 48.85, "lon": 2.35},
    "London": {"lat": 51.51, "lon": -0.13},
    "Berlin": {"lat": 52.52, "lon": 13.41}
}

# -------------------------------
# ÉTAPE 1 : Extraction
# -------------------------------
def extract():
    raw_data = {}
    for city, coords in cities.items():
        url = (
            f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={coords['lat']}&longitude={coords['lon']}&current_weather=true"
        )
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json().get("current_weather", {})
            raw_data[city] = {
                "temperature": data.get("temperature"),
                "windspeed": data.get("windspeed"),
                "weathercode": data.get("weathercode")
            }
        else:
            raw_data[city] = {
                "temperature": None,
                "windspeed": None,
                "weathercode": None
            }
    return raw_data

# -------------------------------
# ÉTAPE 2 : Transformation
# Timestamp en Europe/Paris + run_key
# -------------------------------
def transform(ti):
    raw_data = ti.xcom_pull(task_ids="extract_task")
    transformed = []

    paris_tz = timezone("Europe/Paris")
    now = datetime.now(paris_tz)
    date_str = now.strftime("%Y-%m-%d")

    for city, data in raw_data.items():
        transformed.append({
            "city": city,
            "timestamp": now.isoformat(),  # ex: 2025-07-02T08:00:00+02:00
            "temp_C": data.get("temperature"),
            "wind_kmh": data.get("windspeed"),
            "weather_code": data.get("weathercode"),
            "run_key": f"{city}_{date_str}"
        })

    df = pd.DataFrame(transformed)
    ti.xcom_push(key="transformed_data", value=df.to_dict(orient="records"))

# -------------------------------
# ÉTAPE 3 : Chargement
# Garde tout l'historique, évite doublons ville + jour
# -------------------------------
def load(ti):
    data = ti.xcom_pull(key="transformed_data", task_ids="transform_task")
    df = pd.DataFrame(data)

    filepath = "/opt/airflow/data/weather_data.csv"
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    if os.path.exists(filepath):
        existing_df = pd.read_csv(filepath)

        # 🕓 S'assurer que tous les timestamps ont le même format
        df["timestamp"] = df["timestamp"].apply(lambda x: parser.isoparse(x).isoformat())
        existing_df["timestamp"] = existing_df["timestamp"].apply(lambda x: parser.isoparse(x).isoformat())

        # 📅 Extraire la date (ignore l'heure/fuseau)
        df["date_only"] = df["timestamp"].apply(lambda x: parser.isoparse(x).date())
        existing_df["date_only"] = existing_df["timestamp"].apply(lambda x: parser.isoparse(x).date())

        # 🔁 Combine et garde 1 ligne par ville + jour
        combined_df = pd.concat([existing_df, df])
        combined_df.drop_duplicates(subset=["city", "date_only"], inplace=True)
    else:
        df["date_only"] = df["timestamp"].apply(lambda x: parser.isoparse(x).date())
        combined_df = df

    # 🧹 Nettoyage avant écriture
    combined_df.drop(columns=["run_key", "date_only"], inplace=True, errors="ignore")
    combined_df.to_csv(filepath, index=False)

# -------------------------------
# DAG configuration
# -------------------------------
with DAG(
    dag_id="weather_etl",
    schedule="0 8 * * *",  # Tous les jours à 8h UTC
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["weather"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load
    )

    extract_task >> transform_task >> load_task
