"""
Week 12 -- Kafka Producer: Greenhouse Sensor Telemetry
=====================================================
Simulates IoT sensors reporting temperature and humidity readings
to a shared Confluent Cloud Kafka topic.  Each message is a JSON
object tagged with your student ID so you can identify your own
data in the shared topic:

    {"student": "alice", "sensor_id": "sensor-03",
     "temperature_c": 24.7, "humidity_pct": 61.2,
     "timestamp": "2026-04-14T09:30:01Z"}

Setup:
    1. Copy  kafka_config_example.py  to  _kafka_config.py
    2. Paste the shared API key and secret provided by your instructor.

Usage:
    python producer.py --student alice                # required: your first name
    python producer.py --student alice --count 100    # send 100 messages
    python producer.py --student alice --burst        # send all instantly
"""

import argparse
import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

# ── Load credentials from local config (gitignored) ─────────────
try:
    from _kafka_config import KAFKA_CONFIG
except ImportError:
    raise SystemExit(
        "Missing _kafka_config.py — copy _kafka_config.py.example "
        "and fill in your Confluent Cloud credentials."
    )

TOPIC = "greenhouse-sensors"
SENSOR_IDS = [f"sensor-{i:02d}" for i in range(1, 7)]  # sensor-01 … sensor-06


def make_reading(student: str, sensor_id: str) -> dict:
    """Generate a single simulated sensor reading tagged with student ID."""
    return {
        "student": student,
        "sensor_id": sensor_id,
        "temperature_c": round(random.uniform(18.0, 35.0), 1),
        "humidity_pct": round(random.uniform(30.0, 90.0), 1),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def delivery_report(err, msg):
    """Called once per message to confirm delivery."""
    if err is not None:
        print(f"  DELIVERY FAILED: {err}")
    else:
        print(
            f"  -> {msg.topic()} [{msg.partition()}] "
            f"offset {msg.offset()}"
        )


def run(student: str, count: int, burst: bool):
    producer = Producer({
        **KAFKA_CONFIG,
        "client.id": f"{student}-producer",
    })

    print(f"[{student}] Producing {count} messages to '{TOPIC}' ...")
    for i in range(count):
        sensor_id = random.choice(SENSOR_IDS)
        reading = make_reading(student, sensor_id)
        value = json.dumps(reading)

        # Key on student + sensor_id so each student's sensors land in
        # consistent partitions without colliding with other students.
        msg_key = f"{student}-{sensor_id}"
        producer.produce(
            TOPIC,
            key=msg_key.encode("utf-8"),
            value=value.encode("utf-8"),
            callback=delivery_report,
        )

        # Trigger delivery reports without blocking
        producer.poll(0)

        if not burst:
            time.sleep(1)

    # Wait for any remaining messages to be delivered
    remaining = producer.flush(timeout=10)
    if remaining > 0:
        print(f"WARNING: {remaining} message(s) were not delivered.")
    else:
        print("All messages delivered.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Greenhouse sensor producer")
    parser.add_argument("--student", required=True,
                        help="your first name (lowercase) -- tags every message")
    parser.add_argument("--count", type=int, default=20, help="messages to send")
    parser.add_argument("--burst", action="store_true", help="send without delay")
    args = parser.parse_args()
    run(args.student.lower().strip(), args.count, args.burst)
