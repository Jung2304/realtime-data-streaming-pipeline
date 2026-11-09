FROM twibot22-airflow:3.0.0

USER airflow

RUN pip install --no-cache-dir \
    "confluent-kafka[json,schemaregistry]==2.16.1"