import json
import duckdb
import requests
from airflow.decorators import task
from airflow.models.dag import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Télécharger les données open sky

OPEN_SKY_COLUMNS = [
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "sensors",
    "geo_altitude",
    "squawk",
    "spi",
    "position_source",
    "category"]

ALL_STATES_URL = "https://opensky-network.org/api/states/all?extended=true"

# Chemins absolus dans le container
# DATA_FILE_PATH = "/opt/airflow/dags/data/data.json"
# DB_FILE_PATH = "/opt/airflow/dags/data/bdd_airflow"


def to_dict(states_list, columns, timestamp):
    out = []
    for state in states_list:
        state_dict = dict(zip(columns, state))
        state_dict["timestamp"] = timestamp
        out.append(state_dict)
    return out


@task(multiple_outputs=True)
def get_flight_data(columns, url):
    req = requests.get(url)
    response = req.json()

    if req.status_code != 200:
        raise Exception(f"Erreur API OpenSky ({req.status_code}) : {response.get('message')}")

    timestamp = response.get("time", None)
    states_list = response.get("states", [])
    states_json = to_dict(states_list, columns, timestamp)

    data_file_path = f"/opt/airflow/dags/data/data_{timestamp}.json"

    with open(data_file_path, "w") as file:
        json.dump(states_json, file)

    print(f"Données téléchargées dans {data_file_path}")
    return {"file_name": data_file_path, "timestamp": timestamp, "rows": len(states_list)}


def load_from_file():
    return SQLExecuteQueryOperator(
        task_id="load_from_file",
        conn_id="DUCK_DB",
        # sql="""
        # INSERT INTO bdd_airflow.main.openskynetwork_brute
        # (SELECT * FROM '{{ ti.xcom_pull(task_ids="get_flight_data", key="file_name") }}')
        # """,
        sql="load_from_file.sql",
        return_last=True,
        show_return_value_in_logs=True
    )


@task()
def check_row_numbers(ti=None):
    expected_lines = ti.xcom_pull(task_ids='get_flight_data', key='rows')
    lines_found = ti.xcom_pull(task_ids='load_from_file', key='return_value')[0][0]

    if lines_found != expected_lines:
        raise Exception(f"Nombre de lignes chargees ({lines_found}) != nombre de lignes de l'API ({expected_lines})")

    print(f"Nombre de lignes = {lines_found}")


def check_duplicates():
    return SQLExecuteQueryOperator(
        task_id="check_duplicates",
        conn_id="DUCK_DB",
        sql="check_duplicates.sql",
        return_last=True,
        show_return_value_in_logs=True
    )


@dag()
def flights_pipeline():
    (
            EmptyOperator(task_id="start")
            >> get_flight_data(OPEN_SKY_COLUMNS, ALL_STATES_URL)
            >> load_from_file()
            >> [check_row_numbers(), check_duplicates()]
            >> EmptyOperator(task_id="end")
    )


flight_pipeline_dag = flights_pipeline()
