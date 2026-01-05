import json
import time
import pandas as pd
import duckdb
import requests

from datetime import datetime

from airflow.decorators import task, task_group
from airflow.models import Param
from airflow.models.dag import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.filesystem import FileSensor

# Télécharger les données open sky

apis_list = [
    {"schedule": "30 9 * * *",
     "name": "states",
     "url": "https://opensky-network.org/api/states/all?extended=true",
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
     "timestamp_required": False,
     },
    {"schedule": "30 14 * * *",
     "name": "flights",
     "url": "https://opensky-network.org/api/flights/all?begin={begin}&end={end}",
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
     "timestamp_required": True,
     }
]


# Chemins absolus dans le container
# data_file_name = "/opt/airflow/dags/data/data.json"
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


def format_datetime(input_datetime):
    return input_datetime.strftime("%Y%m%d")


@task(multiple_outputs=True)
def run_parameters(api, dag_run=None):
    out = api

    data_interval_start = format_datetime(dag_run.data_interval_start)
    data_interval_end = format_datetime(dag_run.data_interval_end)

    data_file_name = f"/opt/airflow/dags/data/data_{out['name']}_{data_interval_start}_{data_interval_end}.json"
    out["data_file_name"] = data_file_name

    # SQL pour charger les données dans la DWH
    with open("/opt/airflow/dags/load_from_file.sql", "r") as file:
        load_from_file_sql = file.read().format(target_table=out["target_table"], data_file_name=data_file_name)
    out["load_from_file_sql"] = load_from_file_sql

    if out["timestamp_required"]:
        end = int(time.time())
        begin = end - 3600
        out["url"] = out["url"].format(begin=begin, end=end)

    return {"run_params": out}


@task_group
def data_ingestion_tg(run_params):
    flight_data = get_flight_data(run_params)

    branch = choose_branch(flight_data["rows"])

    insert = insert_data(run_params, flight_data["json_data"])
    load = load_from_file(run_params)

    branch >> [insert, load]


@task(multiple_outputs=True)
def get_flight_data(run_params):
    # Retrouve les paramètres de la tâche run_parameters
    url = run_params["url"]
    columns = run_params["columns"]
    data_file_name = run_params["data_file_name"]

    # Télécharge les données
    req = requests.get(url)
    req.raise_for_status()
    response = req.json()

    # Transforme les données selon l'API d'origine
    if "states" in response:
        timestamp = response.get("time", None)
        results_json = states_to_dict(response.get("states", []), columns, timestamp)
    else:
        timestamp = int(time.time())
        results_json = flights_to_dict(response, timestamp)

    # data_file_name = f"/opt/airflow/dags/data/data_{timestamp}.json"

    with open(data_file_name, "w") as file:
        json.dump(results_json, file)
    rows = len(results_json)
    json_data = results_json if rows < 600 else []
    return {"data_file_name": data_file_name, "timestamp": timestamp, "rows": rows, "json_data": json_data}


@task
def insert_data(run_params, json_data):
    with duckdb.connect("/opt/airflow/dags/data/bdd_airflow") as conn:
        df = pd.DataFrame(json_data)
        conn.register("df_opensky", df)
        conn.sql(f"""
            INSERT INTO {run_params["target_table"]}
            SELECT * FROM df_opensky
        """)


@task()
def load_from_file(run_params):
    with duckdb.connect("/opt/airflow/dags/data/bdd_airflow") as conn:
        conn.sql(run_params["load_from_file_sql"])


@task.branch
def choose_branch(rows):
    if rows < 600:
        return "data_ingestion_tg.insert_data"
    else:
        return "data_ingestion_tg.load_from_file"



@task_group
def data_quality_tg():
    check_row_numbers()
    check_duplicates()


@task()
def check_row_numbers(ti=None):
    expected_lines = ti.xcom_pull(task_ids='data_ingestion_tg.get_flight_data', key='rows')
    lines_found = ti.xcom_pull(task_ids='data_ingestion_tg.load_from_file', key='return_value')[0][0]

    # print(f"Lines found : {lines_found}")

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
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
    concurrency=1,
)
def flights_pipeline():
    run_parameters_task = run_parameters.expand(api=apis_list)
    (
            EmptyOperator(task_id="start")
            >> run_parameters_task
            >> data_ingestion_tg.expand_kwargs(run_parameters_task)
            # >> data_quality_tg()
            >> EmptyOperator(task_id="end")
    )


flights_pipeline()
