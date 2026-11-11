
import time
import pendulum

from airflow.sdk import dag, task


@dag(
    dag_id="test_local_executor",
    start_date=pendulum.datetime(
        2026, 11, 1, tz="Asia/Ho_Chi_Minh"
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["debug", "executor"],
)
def test_local_executor():

    @task
    def sleep_task_a():
        print("Task A started", flush=True)
        time.sleep(120)
        print("Task A completed", flush=True)

    @task
    def sleep_task_b():
        print("Task B started", flush=True)
        time.sleep(120)
        print("Task B completed", flush=True)

    sleep_task_a()
    sleep_task_b()


test_local_executor()
