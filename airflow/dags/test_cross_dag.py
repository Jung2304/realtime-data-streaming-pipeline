
import time
import pendulum

from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)

START_DATE = pendulum.datetime(
    2026, 11, 1, tz="Asia/Ho_Chi_Minh"
)


@dag(
    dag_id="debug_child",
    start_date=START_DATE,
    schedule=None,
    catchup=False,
    max_active_runs=1,
)
def child():

    @task
    def simple_task():
        print("Child started")
        time.sleep(10)
        print("Child completed")

    simple_task()


@dag(
    dag_id="debug_parent",
    start_date=START_DATE,
    schedule=None,
    catchup=False,
    max_active_runs=1,
)
def parent():

    TriggerDagRunOperator(
        task_id="trigger_child",
        trigger_dag_id="randomuser_ingestion",
        wait_for_completion=True,
        deferrable=False,
        poke_interval=5,
    )


child()
parent()
