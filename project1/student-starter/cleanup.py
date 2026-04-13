"""
cleanup.py
==========
Purges both SQS queues and recreates the DynamoDB table between test runs.
Run this before each fresh test to start with a clean slate.

Usage:
    python cleanup.py --student-id macfarlane [--region us-east-1]
"""

import argparse
import time

import boto3

def main():
    parser = argparse.ArgumentParser(description="AdFlow pipeline cleanup")
    parser.add_argument("--student-id", required=True, help="Your student ID (e.g. macfarlane)")
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args()

    sid    = args.student_id
    region = args.region

    sqs     = boto3.client("sqs",     region_name=region)
    dynamo  = boto3.client("dynamodb",region_name=region)

    # ── Purge queues ─────────────────────────────────────────────────────────
    for queue_name in [f"adflow-{sid}-input", f"adflow-{sid}-results"]:
        try:
            url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
            sqs.purge_queue(QueueUrl=url)
            print(f"Purged queue: {queue_name}")
        except sqs.exceptions.QueueDoesNotExist:
            print(f"Queue not found (skipping): {queue_name}")
        except Exception as exc:
            print(f"Error purging {queue_name}: {exc}")

    # ── Recreate DynamoDB table ───────────────────────────────────────────────
    table_name = f"adflow-{sid}-results"
    try:
        dynamo.delete_table(TableName=table_name)
        print(f"Deleting table: {table_name} — waiting for deletion...")
        waiter = dynamo.get_waiter("table_not_exists")
        waiter.wait(TableName=table_name)
        print("Table deleted.")
    except dynamo.exceptions.ResourceNotFoundException:
        print(f"Table not found (skipping delete): {table_name}")

    dynamo.create_table(
        TableName=table_name,
        AttributeDefinitions=[{"AttributeName": "opportunity_id", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "opportunity_id", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )
    print(f"Created table: {table_name}")
    print("\nCleanup complete — ready for a fresh test run.")


if __name__ == "__main__":
    main()
