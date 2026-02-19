"""
Teardown Competition Infrastructure
====================================

Removes the shared SNS topic and all its subscriptions.
Safe to run repeatedly -- idempotent on missing resources.

Usage:
    python teardown_competition.py --region us-east-1
"""

import argparse
import boto3
from botocore.exceptions import ClientError


TOPIC_NAME = "adflow-competition"


def find_topic_arn(sns, topic_name):
    """Find the ARN of a topic by name. Returns None if not found."""
    paginator = sns.get_paginator("list_topics")
    for page in paginator.paginate():
        for topic in page.get("Topics", []):
            if topic["TopicArn"].endswith(f":{topic_name}"):
                return topic["TopicArn"]
    return None


def delete_topic(region):
    """Delete the competition SNS topic and all subscriptions."""
    sns = boto3.client("sns", region_name=region)

    topic_arn = find_topic_arn(sns, TOPIC_NAME)
    if topic_arn is None:
        print(f"Topic '{TOPIC_NAME}' not found -- nothing to tear down.")
        return

    # Unsubscribe all subscriptions first
    paginator = sns.get_paginator("list_subscriptions_by_topic")
    unsub_count = 0
    try:
        for page in paginator.paginate(TopicArn=topic_arn):
            for sub in page.get("Subscriptions", []):
                sub_arn = sub["SubscriptionArn"]
                if sub_arn != "PendingConfirmation":
                    sns.unsubscribe(SubscriptionArn=sub_arn)
                    unsub_count += 1
    except ClientError as e:
        print(f"Warning during unsubscribe: {e}")

    print(f"Removed {unsub_count} subscription(s)")

    # Delete the topic
    sns.delete_topic(TopicArn=topic_arn)
    print(f"Deleted SNS topic: {TOPIC_NAME} ({topic_arn})")


def main():
    parser = argparse.ArgumentParser(description="Teardown AdFlow competition infrastructure")
    parser.add_argument("--region", default="us-east-1", help="AWS region (default: us-east-1)")
    args = parser.parse_args()

    delete_topic(args.region)
    print("Teardown complete.")


if __name__ == "__main__":
    main()
