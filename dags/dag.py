import json
import time

import requests
from airflow.decorators import task
from airflow.models import Param
from airflow.models.dag import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Télécharger les données open sky

endpoint_to_params = {
    "states": {
        "url": "https://opensky-network.org/api/states/all?extended=true",
        "file_name": "/opt/airflow/dags/data/data_{timestamp}.json",
        "columns": [
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
            "category"],
        "target_table": "bdd_airflow.main.openskynetwork_brute",
        "timestamp_required": False

    },
    "flights": {
        "url": "https://opensky-network.org/api/flights/all?begin={begin}&end={end}",
        "file_name": "/opt/airflow/dags/data/data_{timestamp}.json",
        "columns": ['icao24',
                    'firstSeen',
                    'estDepartureAirport',
                    'lastSeen',
                    'estArrivalAirport',
                    'callsign',
                    'estDepartureAirportHorizDistance',
                    'estDepartureAirportVertDistance',
                    'estArrivalAirportHorizDistance',
                    'estArrivalAirportVertDistance',
                    'departureAirportCandidatesCount',
                    'arrivalAirportCandidatesCount'],
        "target_table": "bdd_airflow.main.flights_brute",
        "timestamp_required": True
    }
}


# Chemins absolus dans le container
# DATA_FILE_PATH = "/opt/airflow/dags/data/data.json"
# DB_FILE_PATH = "/opt/airflow/dags/data/bdd_airflow"


def states_to_dict(states_list, columns, timestamp):
    out = []
    for state in states_list:
        state_dict = dict(zip(columns, state))
        state_dict["timestamp"] = timestamp
        out.append(state_dict)
    return out


def flights_to_dict(flights, timestamp):
    out = []
    for flight in flights:
        flight["timestamp"] = timestamp
        out.append(flight)
    return out


@task(multiple_outputs=True)
def run_parameters(params=None):
    out = endpoint_to_params[params["endpoint"]]
    timestamp = int(time.time())
    if out["timestamp_required"]:
        begin = timestamp - 3600
        out["url"] = out["url"].format(begin=begin, end=timestamp)

    out["file_name"] = out["file_name"].format(timestamp=timestamp)

    return out


@task(multiple_outputs=True)
def get_flight_data(ti=None):
    url = ti.xcom_pull(task_ids="run_parameters", key="url")
    columns = ti.xcom_pull(task_ids="run_parameters", key="columns")
    data_file_path = ti.xcom_pull(task_ids="run_parameters", key="file_name")

    req = requests.get(url)
    req.raise_for_status()
    response = req.json()

    if "states" in response:
        timestamp = response.get("time", None)
        results_json = states_to_dict(response.get("states", []), columns, timestamp)
    else:
        timestamp = int(time.time())
        results_json = flights_to_dict(response, timestamp)

    # data_file_path = f"/opt/airflow/dags/data/data_{timestamp}.json"

    with open(data_file_path, "w") as file:
        json.dump(results_json, file)

    return {"file_name": data_file_path, "timestamp": timestamp, "rows": len(results_json)}


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


@dag(
    params={
        "endpoint": Param(
            default="states",
            enum=list(
                endpoint_to_params.keys()
            )
        )
    }
)
def flights_pipeline():
    (
            EmptyOperator(task_id="start")
            >> run_parameters()
            >> get_flight_data()
            >> load_from_file()
            >> [check_row_numbers(), check_duplicates()]
            >> EmptyOperator(task_id="end")
    )


flight_pipeline_dag = flights_pipeline()
