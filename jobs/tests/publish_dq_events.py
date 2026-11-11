import json
import os
import uuid

from confluent_kafka import (
    Consumer,
    Producer,
    TopicPartition,
    OFFSET_BEGINNING,
)


BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
TOPIC = os.environ["KAFKA_TOPIC"]


def get_sample_message():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "dq-test-sample-reader",
        "enable.auto.commit": False,
    })

    try:
        consumer.assign([
            TopicPartition(TOPIC, 0, OFFSET_BEGINNING)
        ])

        for _ in range(20):
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                raise RuntimeError(msg.error())

            raw = msg.value()

            if not raw or len(raw) <= 5 or raw[0] != 0:
                continue

            try:
                data = json.loads(raw[5:].decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                continue

            if isinstance(data, dict) and "user_id" in data:
                return data, raw[:5]

        raise RuntimeError("No valid sample Kafka message found")

    finally:
        consumer.close()


def main():
    sample, header = get_sample_message()

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",
    })

    delivery_errors = []

    def on_delivery(err, msg):
        if err:
            delivery_errors.append(str(err))

    def make_user():
        user = sample.copy()
        user["user_id"] = str(uuid.uuid4())
        return user

    def send(label, user, key=None):
        kafka_key = key if key is not None else user["user_id"]

        payload = (
            header
            + json.dumps(user, ensure_ascii=False).encode("utf-8")
        )

        producer.produce(
            TOPIC,
            key=kafka_key.encode("utf-8"),
            value=payload,
            on_delivery=on_delivery,
        )

        print(f"Queued test: {label}")

    # 1. Valid record
    valid_user = make_user()
    send("VALID", valid_user)

    # 2. Invalid UUID
    user = make_user()
    user["user_id"] = "not-a-uuid"
    send("INVALID_UUID", user)

    # 3. Missing first name
    user = make_user()
    user["first_name"] = ""
    send("MISSING_FIRST_NAME", user)

    # 4. Invalid email
    user = make_user()
    user["email"] = "invalid-email"
    send("INVALID_EMAIL", user)

    # 5. Kafka key mismatch
    user = make_user()
    send(
        "KEY_MISMATCH",
        user,
        key=str(uuid.uuid4()),
    )

    # 6. Invalid Confluent envelope
    producer.produce(
        TOPIC,
        key=b"invalid-envelope",
        value=b"not-confluent-encoded",
        on_delivery=on_delivery,
    )

    print("Queued test: INVALID_ENVELOPE")

    remaining = producer.flush(30)

    if remaining or delivery_errors:
        raise RuntimeError(
            f"Kafka publish failed: "
            f"remaining={remaining}, errors={delivery_errors}"
        )

    print("All 6 test events published successfully")
    print(f"Expected valid user_id: {valid_user['user_id']}")


if __name__ == "__main__":
    main()
