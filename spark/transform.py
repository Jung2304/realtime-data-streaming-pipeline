import json
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import (
  StructType,
  StructField,
  StringType,
)

def transform_users(kafka_df, schema_path):
  # Read JSON Schema
  definition = json.loads(
    Path(schema_path).read_text(encoding="utf-8")
  )

  spark_schema = StructType([
    StructField(name, StringType(), True)
    for name in definition["properties"]
  ])

  # Validate Confluent serialization header
  valid_envelope = F.coalesce(
    (F.length(F.col("value")) > 5)
    & (F.hex(F.substring("value", 1, 1)) == "00"),
    F.lit(False),
  )

  # Remove 5-byte header and deserialize JSON
  payload = F.when(
    valid_envelope,
    F.expr(
      "CAST(substring(value, 6, length(value) - 5) AS STRING)"
    ),
  )

  return (
    kafka_df
    .select(
      F.from_json(payload, spark_schema).alias("user"),
      F.col("key").cast("string").alias("kafka_key"),
      valid_envelope.alias("valid_envelope"),
    )
    .select(
      "user.*",
      "kafka_key",
      "valid_envelope",
    )
  )
