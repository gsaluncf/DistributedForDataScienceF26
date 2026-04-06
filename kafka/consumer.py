"""
Week 12 -- Kafka Consumer: Greenhouse Sensor Telemetry
=====================================================
Reads JSON sensor messages from the shared 'greenhouse-sensors' topic.
Uses your --student name to create an isolated consumer group so you
do not interfere with other students on the same cluster.

Only messages tagged with your student ID are displayed (others are
silently skipped).

Setup:
    1. Copy  kafka_config_example.py  to  _kafka_config.py
    2. Paste the shared API key and secret provided by your instructor.

Usage:
    python consumer.py --student alice                    # your own group
    python consumer.py --student alice --group teamwork   # named group (prefixed)
    python consumer.py --student alice --all              # show ALL students' data
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


def run(student: str, group_id: str, show_all: bool):
    consumer = Consumer({
        **KAFKA_CONFIG,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })

    consumer.subscribe([TOPIC])
    filter_msg = "showing ALL students" if show_all else f"filtering to '{student}' only"
    print(f"Consumer '{group_id}' subscribed to '{TOPIC}' ({filter_msg}).  Ctrl-C to stop.\n")

    # Running stats per sensor
    stats: dict[str, list[float]] = defaultdict(list)
    msg_count = 0
    skipped = 0

    while running:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Reached end of partition -- not an error
                continue
            print(f"Consumer error: {msg.error()}")
            break

        # Decode the JSON payload
        try:
            reading = json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            print(f"  Skipping bad message: {exc}")
            continue

        # Filter to this student's messages unless --all is set
        msg_student = reading.get("student", "")
        if not show_all and msg_student != student:
            skipped += 1
            continue

        sensor_id = reading["sensor_id"]
        temp = reading["temperature_c"]
        humidity = reading["humidity_pct"]
        ts = reading["timestamp"]

        stats[sensor_id].append(temp)
        avg = sum(stats[sensor_id]) / len(stats[sensor_id])
        msg_count += 1

        owner = f"{msg_student}/" if show_all else ""
        print(
            f"  [{msg.partition()}:{msg.offset():>5}]  "
            f"{owner}{sensor_id}  {temp:5.1f} C  {humidity:5.1f}% RH  "
            f"(avg {avg:5.1f} C over {len(stats[sensor_id])} readings)  "
            f"@ {ts}"
        )

    # -- Summary --
    print(f"\n--- Session summary ({msg_count} messages, {skipped} skipped) ---")
    for sid in sorted(stats):
        readings = stats[sid]
        avg = sum(readings) / len(readings)
        print(f"  {sid}: {len(readings)} readings, avg {avg:.1f} C")

    consumer.close()
    print("Consumer closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Greenhouse sensor consumer")
    parser.add_argument("--student", required=True,
                        help="your first name (lowercase) -- isolates your consumer group")
    parser.add_argument("--group", default=None,
                        help="consumer group suffix (default: monitors)")
    parser.add_argument("--all", dest="show_all", action="store_true",
                        help="show messages from ALL students, not just yours")
    args = parser.parse_args()

    student = args.student.lower().strip()
    group_suffix = args.group if args.group else "monitors"
    group_id = f"{student}-{group_suffix}"

    run(student, group_id, args.show_all)
