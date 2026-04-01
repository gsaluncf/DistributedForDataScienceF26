# ── Confluent Cloud Kafka Credentials ────────────────────────────
# Copy this file to  _kafka_config.py  and fill in your values.
# DO NOT commit _kafka_config.py to git.

KAFKA_CONFIG = {
    "bootstrap.servers": "YOUR_CLUSTER.us-east-1.aws.confluent.cloud:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms":   "PLAIN",
    "sasl.username":     "YOUR_API_KEY",
    "sasl.password":     "YOUR_API_SECRET",
}
