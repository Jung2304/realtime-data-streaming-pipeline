from pyspark.sql import DataFrame
from pyspark.sql import functions as F

UUID_PATTERN = (
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{12}$"
)

EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"

def apply_quality_rules(df: DataFrame) -> DataFrame:

  invalid_uuid = (
    F.col("user_id").isNull()
    | ~F.col("user_id").rlike(UUID_PATTERN)
  )

  missing_name = (
    F.col("first_name").isNull()
    | (F.trim(F.col("first_name")) == "")
  )

  invalid_email = (
    F.col("email").isNull()
    | ~F.col("email").rlike(EMAIL_PATTERN)
  )

  key_mismatch = (
    F.col("kafka_key").isNull()
    | (F.col("user_id") != F.col("kafka_key"))
  )

  invalid_envelope = ~F.col("valid_envelope")

  return df.withColumn(
    "dq_error",
    F.when(invalid_envelope, "INVALID_ENVELOPE")          # invalid Confluent header
    .when(invalid_uuid, "INVALID_UUID")                   # missing or wrong UUID
    .when(missing_name, "MISSING_FIRST_NAME")             
    .when(invalid_email, "INVALID_EMAIL")                 
    .when(key_mismatch, "KEY_MISMATCH")                   # UUID not match Kafka key
    .otherwise(F.lit(None).cast("string"))
  )
