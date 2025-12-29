SELECT callsign, time_position, last_contact, count(*) AS cnt
FROM bdd_airflow.main.openskynetwork_brute
GROUP BY 1, 2, 3
HAVING cnt > 1;
