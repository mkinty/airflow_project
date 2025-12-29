SELECT callsign, timestamp, count(*) AS cnt
FROM {{ ti.xcom_pull(task_ids="run_parameters", key="target_table") }}
GROUP BY 1, 2
HAVING cnt > 1;
