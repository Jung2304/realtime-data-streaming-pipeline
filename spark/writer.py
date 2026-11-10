# Write data to Cassandra
def start_cassandra_stream(users_df, config):

  # DataFrameWrite API (batch)
  def write_batch(batch_df, batch_id):
    (
      batch_df.write
      .format("org.apache.spark.sql.cassandra")     # use Cassandra Connector (identify the connector/data source used by Spark)
      .mode("append")
      .options(
        keyspace="spark_streams",
        table="created_users",
      )
      .save()
    )
    print(f"Cassandra batch {batch_id} completed")

  # DataStreamWriter API (streaming)
  return (
    users_df.writeStream

    # allows us to use the standard DataFrameWriter to write each micro-batch to Cassandra.
    .foreachBatch(write_batch)
    .outputMode("append")
    .option("checkpointLocation", config.checkpoint)

    # process the existing messages and then terminate.
    .trigger(availableNow=True)
    .start()
  )