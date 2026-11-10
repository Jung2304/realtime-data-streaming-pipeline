import pendulum

from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import (
  SparkSubmitOperator,
)

@dag(
  dag_id="check_spark_kafka_connector",
  start_date=pendulum.datetime(
    2026, 11, 1, tz="Asia/Ho_Chi_Minh"
  ),
  schedule=None,
  catchup=False,
  tags=["spark", "kafka", "test"],
)
def check_spark_kafka():

  SparkSubmitOperator(
    task_id="check_kafka_connector",
    conn_id="spark_default",

    application=(
      "/opt/airflow/project/spark/jobs"
      "check_kafka_connector.py"
    ),

    deploy_mode="client",

    packages=(
      "org.apache.spark:"
      "spark-sql-kafka-0-10_2.12:3.5.3"
    ),

    conf={
      "spark.driver.host": "airflow-scheduler",
      "spark.driver.bindAddress": "0.0.0.0",
      "spark.pyspark.python": "python3.12",
      "spark.pyspark.driver.python": "python3.12",
    },
  )

check_spark_kafka()
