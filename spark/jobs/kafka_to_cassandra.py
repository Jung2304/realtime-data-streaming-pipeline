from spark.config import load_settings
from spark.session import create_spark_session
from spark.reader import read_kafka
from spark.transform import transform_users
from spark.writer import start_cassandra_stream

def main():
  config = load_settings()
  spark = create_spark_session(config)

  try:
    # Extract
    kafka_df = read_kafka(spark, config)

    # Transform
    users_df = transform_users(
      kafka_df,
      config.schema_path,
    )

    # Load
    query = start_cassandra_stream(
      users_df,
      config,
    )

    query.awaitTermination()

  finally:
    spark.stop()


if __name__ == "__main__":
  main()