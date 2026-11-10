from pyspark.sql import SparkSession

spark = (
  SparkSession.builder
  .appName("KafkaConnectorCheck")
  .getOrCreate()
)

df = (
  spark.readStream
  .format("kafka")
  .option(
    "kafka.bootstrap.servers",
    "kafka-1:29092,kafka-2:29092,kafka-3:29092"
  )
  .option("subscribe", "randomuser_events_v2")
  .load()
)

df.printSchema()

spark.stop()
