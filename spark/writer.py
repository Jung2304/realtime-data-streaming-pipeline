# Write data to Cassandra
from pyspark import StorageLevel
from pyspark.sql import functions as F

from spark.quality import apply_quality_rules


CASSANDRA_COLUMNS = [
  "user_id",
  "first_name",
  "last_name",
  "gender",
  "address",
  "postcode",
  "email",
  "username",
  "dob",
  "registered_date",
  "phone",
  "picture",
]

def start_cassandra_stream(users_df, config):

  def write_batch(batch_df, batch_id):

    # 1. Apply data quality rules
    checked_df = (
      apply_quality_rules(batch_df)
      .persist(StorageLevel.MEMORY_AND_DISK)    # reuse previously computed data when we perform statistical analysis.
    )

    try:
      # 2. Count valid and invalid records
      results = (
        checked_df
        .groupBy("dq_error")
        .count()
        .collect()
      )

      error_counts = {
        row["dq_error"]: row["count"]
        for row in results
      }

      total = sum(error_counts.values())
      valid_count = error_counts.get(None, 0)
      invalid_count = total - valid_count

      print(
        f"[Batch {batch_id}] "
        f"Total={total}, "
        f"Valid={valid_count}, "
        f"Invalid={invalid_count}",
        flush=True,
      )

      # 3. Log invalid records by error reason
      for error, count in error_counts.items():
        if error is not None:
          print(
            f"[DQ] {error}: {count}",
            flush=True,
          )

      # 4. Skip empty/fully invalid batches
      if valid_count == 0:
        print(
          f"[Batch {batch_id}] "
          "No valid records to write",
          flush=True,
        )
        return

      # 5. Write only valid records to Cassandra
      valid_df = (
        checked_df
        .filter(F.col("dq_error").isNull())
        .select(*CASSANDRA_COLUMNS)
      )

      (
        valid_df.write
        .format("org.apache.spark.sql.cassandra")       # use Cassandra Connector (identify the connector/data source used by Spark)
        .mode("append")
        .options(
          keyspace="spark_streams",
          table="created_users",
        )
        .save()
      )

      print(
        f"Cassandra batch {batch_id} completed",
        flush=True,
      )

    finally:
      checked_df.unpersist()      # unpersist() releases cached data after completion

  return (
    users_df.writeStream
    .foreachBatch(write_batch)
    .outputMode("append")
    .option(
      "checkpointLocation",
      config.checkpoint,
    )
    .trigger(availableNow=True)
    .start()
  )
