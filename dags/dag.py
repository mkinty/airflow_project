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
# DATA_FILE_PATH = "/opt/airflow/dags/data/data.json"
DB_FILE_PATH = "/opt/airflow/dags/data/bdd_airflow.duckdb"


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


@task()
def load_from_file(db_file_path, ti=None):
    conn = None
    data_file_path = ti.xcom_pull(task_ids='get_flight_data', key='file_name')
    try:
        conn = duckdb.connect(db_file_path)
        # Créer la table si elle n'existe pas et insérer les données
        # conn.sql(f"""
        # CREATE TABLE IF NOT EXISTS opskynetwork_brute AS
        # SELECT * FROM read_json_auto('{data_file_path}')
        # """)
        # Une fois la table créée, il faut simplement inserer les novelles données
        conn.sql(f"""
                INSERT INTO bdd_airflow.main.opskynetwork_brute
                (SELECT * FROM read_json_auto('{data_file_path}'))
                """)
        print(f"Données insérées dans {db_file_path}")
    except Exception as e:
        print("Erreur DuckDB:", e)
    finally:
        if conn:
            conn.close()


@task()
def check_row_numbers(db_file_path, ti=None):
    conn = None
    lines_found = 0
    xcom_content = ti.xcom_pull(task_ids='get_flight_data', key='return_value')
    timestamp = xcom_content.get("timestamp", None)
    expected_lines = xcom_content.get("rows", None)
    try:
        conn = duckdb.connect(db_file_path, read_only=True)  # lecture seule pour éviter le vérou
        lines_found = conn.sql(f"SELECT count(*) FROM bdd_airflow.main.opskynetwork_brute WHERE timestamp={timestamp}").fetchone()[0]
    finally:
        if conn:
            conn.close()

    if lines_found != expected_lines:
        raise Exception(f"Nombre de lignes chargees ({lines_found}) != nombre de lignes de l'API ({expected_lines})")

    print(f"Nombre de lignes = {lines_found}")


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
            >> get_flight_data(OPEN_SKY_COLUMNS, ALL_STATES_URL)
            >> load_from_file(DB_FILE_PATH)
            >> [check_row_numbers(DB_FILE_PATH), check_duplicates(DB_FILE_PATH)]
            >> EmptyOperator(task_id="end")
    )


flight_pipeline_dag = flights_pipeline()
