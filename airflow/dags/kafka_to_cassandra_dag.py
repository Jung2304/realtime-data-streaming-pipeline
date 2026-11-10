import pendulum

from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import (
  SparkSubmitOperator,
)

@dag(
  dag_id="kafka_to_cassandra",
  start_date=pendulum.datetime(
    2026, 11, 1, tz="Asia/Ho_Chi_Minh"
  ),
  schedule=None,
  catchup=False,
  max_active_runs=1,
  tags=["spark", "kafka", "cassandra"],
)
def kafka_to_cassandra_pipeline():

  SparkSubmitOperator(
    task_id="stream_kafka_to_cassandra",
    conn_id="spark_default",

    application=(
      "/opt/airflow/project/spark/jobs/"
      "kafka_to_cassandra.py"
    ),

    deploy_mode="client",

    packages=(
      "org.apache.spark:"
      "spark-sql-kafka-0-10_2.12:3.5.3,"          # Spark reads Kafka
      "com.datastax.spark:"
      "spark-cassandra-connector_2.12:3.5.1"      # Spark writes Cassandra
    ),

    executor_cores=2,
    executor_memory="1g",

    conf={
      "spark.driver.host": "airflow-scheduler",
      "spark.driver.bindAddress": "0.0.0.0",
      "spark.pyspark.python": "python3.12",
      "spark.pyspark.driver.python": "python3.12",
    },
  )

kafka_to_cassandra_pipeline()
