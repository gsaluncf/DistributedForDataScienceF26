"""
Week 12 — Kafka Consumer: Greenhouse Sensor Telemetry
=====================================================
Reads JSON sensor messages from the 'greenhouse-sensors' topic,
prints each reading, and tracks a running average temperature
per sensor.

Setup:
    1. Copy  _kafka_config.py.example  to  _kafka_config.py
    2. Fill in your Confluent Cloud bootstrap server, API key, and secret.

Usage:
    python consumer.py                          # default group
    python consumer.py --group team-alpha       # named consumer group
    python consumer.py --group team-alpha &     # run multiple in parallel
    python consumer.py --group team-alpha &     #   to see partition rebalancing
"""

import argparse
import json
import signal
import sys
from collections import defaultdict

from confluent_kafka import Consumer, KafkaError

# ── Load credentials from local config (gitignored) ─────────────
try:
    from _kafka_config import KAFKA_CONFIG
except ImportError:
    raise SystemExit(
        "Missing _kafka_config.py — copy _kafka_config.py.example "
        "and fill in your Confluent Cloud credentials."
    )

TOPIC = "greenhouse-sensors"

# Graceful shutdown on Ctrl-C
running = True


def signal_handler(_sig, _frame):
    global running
    running = False


signal.signal(signal.SIGINT, signal_handler)


def run(group_id: str):
    consumer = Consumer({
        **KAFKA_CONFIG,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })

    consumer.subscribe([TOPIC])
    print(f"Consumer '{group_id}' subscribed to '{TOPIC}'.  Ctrl-C to stop.\n")

    # Running stats per sensor
    stats: dict[str, list[float]] = defaultdict(list)
    msg_count = 0

    while running:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Reached end of partition — not an error
                continue
            print(f"Consumer error: {msg.error()}")
            break

        # Decode the JSON payload
        try:
            reading = json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            print(f"  Skipping bad message: {exc}")
            continue

        sensor_id = reading["sensor_id"]
        temp = reading["temperature_c"]
        humidity = reading["humidity_pct"]
        ts = reading["timestamp"]

        stats[sensor_id].append(temp)
        avg = sum(stats[sensor_id]) / len(stats[sensor_id])
        msg_count += 1

        print(
            f"  [{msg.partition()}:{msg.offset():>5}]  "
            f"{sensor_id}  {temp:5.1f} C  {humidity:5.1f}% RH  "
            f"(avg {avg:5.1f} C over {len(stats[sensor_id])} readings)  "
            f"@ {ts}"
        )

    # ── Summary ──────────────────────────────────────────────────
    print(f"\n--- Session summary ({msg_count} messages) ---")
    for sid in sorted(stats):
        readings = stats[sid]
        avg = sum(readings) / len(readings)
        print(f"  {sid}: {len(readings)} readings, avg {avg:.1f} C")

    consumer.close()
    print("Consumer closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Greenhouse sensor consumer")
    parser.add_argument(
        "--group", default="greenhouse-monitors",
        help="consumer group ID (use the same group across terminals to see rebalancing)"
    )
    args = parser.parse_args()
    run(args.group)
