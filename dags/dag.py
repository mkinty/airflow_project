from airflow.models.dag import dag
from airflow.operators.empty import EmptyOperator


@dag()
def flights_pipeline():
    (
        EmptyOperator(task_id="start")
        >> EmptyOperator(task_id="end")
    )


flight_pipeline_dag = flights_pipeline()