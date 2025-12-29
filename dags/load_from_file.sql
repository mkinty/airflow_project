INSERT INTO bdd_airflow.main.openskynetwork_brute
SELECT * FROM '{{ ti.xcom_pull(task_ids="get_flight_data", key="file_name") }}'