# Deserialize & validate
import json
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

UUID_PATTERN = (
  r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-"
  r"[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
  r"[0-9a-fA-F]{12}$"
)

def transform_users(kafka_df, schema_path):
    # Read the same JSON Schema used by Producer
    definition = json.loads(
      Path(schema_path).read_text(encoding="utf-8")
    )

    spark_schema = StructType([
      StructField(name, StringType(), True)
      for name in definition["properties"]
    ])

    # Remove the 5-byte Confluent framing
    decoded = (
      kafka_df
      .where(F.length("value") > 5)
      .where(
        F.expr("hex(substring(value, 1, 1)) = '00'")
      )
      .select(
        F.from_json(
          F.expr(
            "CAST(substring(value, 6, "
            "length(value) - 5) AS STRING)"
          ),
          spark_schema,
        ).alias("user"),
        F.col("key").cast("string").alias("kafka_key"),
      )
      .select("user.*", "kafka_key")
    )

    # Validate required fields
    valid = F.lit(True)

    for field in definition["required"]:
      valid = valid & F.col(field).isNotNull()

    return (
      decoded
      .where(valid)
      .where(F.col("user_id").rlike(UUID_PATTERN))
      .where(F.col("user_id") == F.col("kafka_key"))
      .drop("kafka_key")
    )
