import pendulum

from airflow.sdk import dag
from airflow.providers.standard.operators.trigger_dagrun import (
  TriggerDagRunOperator,
)

@dag(
  dag_id="randomuser_batch_pipeline",
  start_date=pendulum.datetime(
    2026, 11, 1, tz="Asia/Ho_Chi_Minh"
  ),
  schedule=None,
  catchup=False,
  max_active_runs=1,
  tags=["orchestration", "kafka", "spark", "cassandra"],
)
def randomuser_batch_pipeline():

  produce_events = TriggerDagRunOperator(
    task_id="produce_events",
    trigger_dag_id="randomuser_ingestion",
    wait_for_completion=True,
    deferrable=True,
    poke_interval=10,     # Check the DAG status, triggered every 10 seconds
    retries=0,
  )

  process_events = TriggerDagRunOperator(
    task_id="process_events",
    trigger_dag_id="kafka_to_cassandra",
    wait_for_completion=True,
    deferrable=True,
    poke_interval=10,
    retries=0,
  )

  produce_events >> process_events


randomuser_batch_pipeline()
