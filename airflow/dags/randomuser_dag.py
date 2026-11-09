import os
import requests

import pendulum
import time
from datetime import timedelta

from airflow.sdk import dag, task
from pathlib import Path

API_URL = "https://randomuser.me/api/"

BATCH_SIZE = 5
FETCH_INTERVAL = 5
STREAM_DURATION = 60

def get_data(batch_size: int = BATCH_SIZE) -> list[dict]:
  response = requests.get(
    API_URL,
    params={"results": batch_size},
    timeout=30,
  )
  response.raise_for_status()

  payload = response.json()

  if "error" in payload:
    raise ValueError(payload["error"])

  results = payload.get("results", [])

  if not results:
    raise ValueError("Random User API returned no results")

  return results


def format_data(res: dict) -> dict:
  location = res["location"]
  street = location["street"]

  return {
    "user_id": res["login"]["uuid"],
    "first_name": res["name"]["first"],
    "last_name": res["name"]["last"],
    "gender": res["gender"],
    "address": (
      f"{street['number']} {street['name']}, "
      f"{location['city']}, "
      f"{location['state']}, "
      f"{location['country']}"
    ),
    "postcode": str(location["postcode"]),
    "email": res["email"],
    "username": res["login"]["username"],
    "dob": res["dob"]["date"],
    "registered_date": res["registered"]["date"],
    "phone": res["phone"],
    "picture": res["picture"]["medium"],
    }


@dag(
  dag_id = "randomuser_ingestion",
  start_date = pendulum.datetime(
    2026, 11, 1, tz = "Asia/Ho_Chi_Minh"
  ),
  schedule = None,
  catchup = False,
  max_active_runs = 1,
  tags = ["randomuser", "ingestion", "kafka"],
)

def ingestion_pipeline():

  @task(execution_timeout=timedelta(minutes=3))
  def stream_data() -> None:

    from confluent_kafka import Producer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.json_schema import JSONSerializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    topic = os.getenv("KAFKA_TOPIC", "randomuser_events_v2")

    # 1. Setup Schema Registry and Serializer
    registry = SchemaRegistryClient({
      "url": os.environ["SCHEMA_REGISTRY_URL"]
    })

    schema_path = Path(__file__).parent / "schemas" / "randomuser.json"

    serializer = JSONSerializer(
      schema_path.read_text(encoding="utf-8"),
      registry,
      conf={"auto.register.schemas": True},
    )

    # 2. Setup Kafka Producer
    producer = Producer({
      "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
      "acks": "all",
    })

    errors = []

    def delivery_report(err, msg):
      if err:
        errors.append(str(err))

    # 3. Fetch and publish users for 60 seconds
    start_time = time.monotonic()
    total_messages = 0

    while time.monotonic() - start_time < STREAM_DURATION:
      users = get_data()

      for raw_user in users:
        user = format_data(raw_user)

        value = serializer(
          user,
          SerializationContext(topic, MessageField.VALUE),
        )

        producer.produce(
          topic=topic,
          key=user["user_id"].encode("utf-8"),
          value=value,
          on_delivery=delivery_report,
        )

        total_messages += 1

      print(f"Queued {len(users)} users")
      time.sleep(FETCH_INTERVAL)

    # 4. Wait for delivery
    remaining = producer.flush(30)

    if remaining or errors:
      raise RuntimeError(
        f"Kafka delivery failed: pending={remaining}, errors={errors}"
      )

    print(f"Successfully published {total_messages} users")

  stream_data()

ingestion_pipeline()
