# Read Kafka

def read_kafka(spark, config):
  return (
    spark.readStream
    .format("kafka")
    .option(
      "kafka.bootstrap.servers",
      config.kafka_servers,
    )
    .option("subscribe", config.kafka_topic)
    .option("startingOffsets", "earliest")      # read from the start if doesn't have checkpoint
    .option("maxOffsetsPerTrigger", 100)        # limit the number of Kafka offsets processed in a micro-batch 
    .load()
  )