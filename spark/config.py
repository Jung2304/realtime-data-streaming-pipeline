import os 
from dataclasses import dataclass

@dataclass(frozen=True)
class Settings:
  kafka_servers: str
  kafka_topic: str
  cassandra_host: str
  cassandra_port: int
  cassandra_dc: str
  checkpoint: str
  schema_path: str

def load_settings() -> Settings:
  return Settings(
    kafka_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    kafka_topic=os.getenv("KAFKA_TOPIC", "randomuser_events_v2"),
    cassandra_host=os.getenv("CASSANDRA_HOST", "cassandra"),
    cassandra_port=int(os.getenv("CASSANDRA_PORT", "9042")),
    cassandra_dc=os.getenv("CASSANDRA_LOCAL_DC", "datacenter1"),
    checkpoint=os.environ["SPARK_CHECKPOINT_DIR"],
    schema_path="/opt/airflow/project/schemas/randomuser.json",
  )