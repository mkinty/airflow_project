import json
import duckdb
import requests
from airflow.decorators import task
from airflow.models.dag import dag
from airflow.operators.empty import EmptyOperator

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
DATA_FILE_PATH = "/opt/airflow/dags/data/data.json"
DB_FILE_PATH = "/opt/airflow/dags/data/bdd_airflow.duckdb"


@task()
def get_flight_data(columns, url, data_file_path):
    req = requests.get(url)
    response = req.json()

    if req.status_code != 200:
        raise Exception(f"Erreur API OpenSky ({req.status_code}) : {response.get('message')}")

    states_list = response.get("states", [])
    states_json = [dict(zip(columns, state)) for state in states_list]

    with open(data_file_path, "w") as file:
        json.dump(states_json, file)
    print(f"Données téléchargées dans {data_file_path}")


@task()
def load_from_file(data_file_path, db_file_path):
    conn = None
    try:
        conn = duckdb.connect(db_file_path)
        # Créer la table si elle n'existe pas et insérer les données
        conn.sql(f"""
        CREATE TABLE IF NOT EXISTS opskynetwork_brute AS
        SELECT * FROM read_json_auto('{data_file_path}')
        """)
        print(f"Données insérées dans {db_file_path}")
    except Exception as e:
        print("Erreur DuckDB:", e)
    finally:
        if conn:
            conn.close()


@task()
def check_row_numbers(db_file_path):
    conn = None
    nb_rows = 0
    try:
        conn = duckdb.connect(db_file_path, read_only=True)  # lecture seule pour éviter le vérou
        nb_rows = conn.sql(f"SELECT count(*) FROM bdd_airflow.main.opskynetwork_brute").fetchone()[0]
    except Exception as e:
        print("Erreur DuckDB:", e)
    finally:
        if conn:
            conn.close()
    print("Nombre de lignes : ", nb_rows)


@task
def check_duplicates(db_file_path):
    conn = None
    nb_duplicates = 0
    try:
        conn = duckdb.connect(db_file_path, read_only=True)
        nb_duplicates = conn.sql("""
        SELECT callsign, time_position, last_contact, count(*) AS cnt
        FROM bdd_airflow.main.opskynetwork_brute
        GROUP BY 1, 2, 3
        HAVING cnt > 1;
        """).count(column="cnt").fetchone()[0]
    except Exception as e:
        print("Erreur DuckDB:", e)
    finally:
        if conn:
            conn.close()
    print("Nombre de lignes : ", nb_duplicates)


@dag()
def flights_pipeline():
    (
            EmptyOperator(task_id="start")
            >> get_flight_data(OPEN_SKY_COLUMNS, ALL_STATES_URL, DATA_FILE_PATH)
            >> load_from_file(DATA_FILE_PATH, DB_FILE_PATH)
            >> [check_row_numbers(DB_FILE_PATH), check_duplicates(DB_FILE_PATH)]
            >> EmptyOperator(task_id="end")
    )


flight_pipeline_dag = flights_pipeline()
