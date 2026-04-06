# ── Confluent Cloud Kafka Credentials ────────────────────────────
# Copy this file to  _kafka_config.py  and fill in the API key
# and secret provided by your instructor.
# DO NOT commit _kafka_config.py to git.

KAFKA_CONFIG = {
    "bootstrap.servers": "pkc-619z3.us-east1.gcp.confluent.cloud:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms":   "PLAIN",
    "sasl.username":     "PASTE_API_KEY_HERE",
    "sasl.password":     "PASTE_API_SECRET_HERE",
}
