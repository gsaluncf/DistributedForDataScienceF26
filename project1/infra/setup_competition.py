"""
Setup Competition Infrastructure
=================================

Creates the shared SNS topic for the fan-out competition phase.
Students subscribe their input queues to this topic so that every
student receives every message simultaneously.

Usage:
    python setup_competition.py --region us-east-1
"""

import argparse
import json
import boto3


TOPIC_NAME = "adflow-competition"


def create_topic(region):
    """Create the competition SNS topic and return its ARN."""
    sns = boto3.client("sns", region_name=region)

    response = sns.create_topic(Name=TOPIC_NAME)
    topic_arn = response["TopicArn"]
    print(f"Created SNS topic: {TOPIC_NAME}")
    print(f"  ARN: {topic_arn}")

    # Set a permissive policy so student queues in other accounts can subscribe.
    # In production you would scope this tightly. For a classroom setting
    # with 25 students this is acceptable.
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowStudentSubscriptions",
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "sns:Subscribe",
                "Resource": topic_arn,
            }
        ],
    }
    sns.set_topic_attributes(
        TopicArn=topic_arn,
        AttributeName="Policy",
        AttributeValue=json.dumps(policy),
    )
    print("  Policy set: open subscription for student accounts")

    return topic_arn


def subscribe_queue(region, topic_arn, queue_arn):
    """Subscribe a student's SQS queue to the competition topic."""
    sns = boto3.client("sns", region_name=region)

    response = sns.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=queue_arn,
    )
    sub_arn = response["SubscriptionArn"]
    print(f"Subscribed queue to competition topic:")
    print(f"  Queue ARN: {queue_arn}")
    print(f"  Subscription ARN: {sub_arn}")
    return sub_arn


def publish_test_message(region, topic_arn):
    """Publish a quick test message to verify the topic works."""
    sns = boto3.client("sns", region_name=region)

    test_msg = json.dumps({
        "opportunity_id": "test-000",
        "timestamp": "2025-03-10T12:00:00Z",
        "user_id": "u_test",
        "content_category": "sports",
        "device_type": "mobile",
        "region": "southeast",
        "bids": [
            {"advertiser_id": "adv_test_01", "bid_amount": 3.50, "category": "sportswear"},
            {"advertiser_id": "adv_test_02", "bid_amount": 4.20, "category": "fast_food"},
        ],
    })

    response = sns.publish(
        TopicArn=topic_arn,
        Message=test_msg,
    )
    print(f"Published test message: MessageId={response['MessageId']}")


def main():
    parser = argparse.ArgumentParser(description="Setup AdFlow competition infrastructure")
    parser.add_argument("--region", default="us-east-1", help="AWS region (default: us-east-1)")
    parser.add_argument(
        "--subscribe-queue-arn", default=None,
        help="Optionally subscribe a student queue ARN to the topic",
    )
    parser.add_argument("--test", action="store_true", help="Publish a test message after creation")
    args = parser.parse_args()

    topic_arn = create_topic(args.region)

    if args.subscribe_queue_arn:
        subscribe_queue(args.region, topic_arn, args.subscribe_queue_arn)

    if args.test:
        publish_test_message(args.region, topic_arn)

    print("\nDone. Share this with students for the competition phase:")
    print(f"  SNS Topic ARN: {topic_arn}")


if __name__ == "__main__":
    main()
