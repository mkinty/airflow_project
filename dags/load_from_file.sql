INSERT INTO {{ ti.xcom_pull(task_ids='run_parameters', key='target_table') }}
(SELECT * FROM '{{ ti.xcom_pull(task_ids='run_parameters', key='file_name') }}')