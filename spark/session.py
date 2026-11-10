# Create SparkSession and config Cassandra connection for Spark Connector 
from pyspark.sql import SparkSession

def create_spark_session(config) -> SparkSession:
  return (
    SparkSession.builder
    .appName("RandomUserKafkaToCassandra")
    .config(
      "spark.cassandra.connection.host",
      config.cassandra_host,
    )
    .config(
      "spark.cassandra.connection.port",
      config.cassandra_port,
    )
    .config(
      "spark.cassandra.connection.localDC",
      config.cassandra_dc,
    )

    # Shuffle is the process of redistributing data across partitions (not random)
    .config("spark.sql.shuffle.partitions", "2")      
    .getOrCreate()
  )