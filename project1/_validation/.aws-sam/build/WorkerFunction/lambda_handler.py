r"""
AdFlow Ad Selection Worker - Reference Implementation
=====================================================

This Lambda function processes ad opportunity messages from an SQS queue,
applies a quality-adjusted scoring function to select winning bids,
posts results to a results queue, and persists them to DynamoDB.

Architecture:
    SQS Input Queue --> Lambda --> SQS Results Queue
                                \-> DynamoDB Results Table

Performance notes:
    - Results are posted to SQS before writing to DynamoDB. The SQS post
      is on the measured latency path (processed_at timestamp). DynamoDB
      writes are persistence and can tolerate slightly more time.
    - Each batch logs wall-clock timing for the full batch and per-message.
    - CloudWatch Logs will show structured log lines. To view them:
        aws logs tail /aws/lambda/adflow-{studentid}-worker --follow
      Or in the Console: CloudWatch > Log groups > /aws/lambda/adflow-{id}-worker

Deployment (SAM):
    sam build
    sam deploy --guided --stack-name adflow-{studentid}
"""

import json
import os
import time
import logging
from datetime import datetime, timezone
from decimal import Decimal

import boto3


# ---------------------------------------------------------------------------
# Logging setup - structured logs for CloudWatch
# ---------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# AWS clients - created once per Lambda cold start for reuse across invocations
# ---------------------------------------------------------------------------
sqs = boto3.client("sqs")
dynamodb = boto3.resource("dynamodb")

RESULTS_QUEUE_URL = os.environ.get("RESULTS_QUEUE_URL", "")
DYNAMO_TABLE_NAME = os.environ.get("DYNAMO_TABLE_NAME", "")

# ---------------------------------------------------------------------------
# Scoring constants
# ---------------------------------------------------------------------------

# Relevance multiplier: (content_category, advertiser_category) -> multiplier
RELEVANCE_MAP = {
    ("sports", "sportswear"): 1.4,
    ("sports", "energy_drink"): 1.3,
    ("finance", "fintech"): 1.5,
    ("finance", "insurance"): 1.3,
    ("entertainment", "streaming"): 1.4,
    ("entertainment", "gaming"): 1.3,
    ("lifestyle", "beauty"): 1.3,
    ("lifestyle", "travel"): 1.2,
}

# Time bonus: (start_hour_inclusive, end_hour_exclusive, bonus)
TIME_WINDOWS = [
    (6, 9, 1.20),     # Morning commute
    (12, 14, 1.15),   # Lunch browsing
    (19, 23, 1.25),   # Evening peak
]

DEVICE_BONUS = {
    "mobile": 1.1,
    "desktop": 1.0,
}


# ---------------------------------------------------------------------------
# Scoring function
# ---------------------------------------------------------------------------

def compute_score(bid, opportunity):
    """
    Compute the quality-adjusted score for a single bid.

    Formula:
        score = bid_amount * relevance_multiplier * time_bonus * device_bonus

    Args:
        bid (dict): Must have keys: advertiser_id, bid_amount, category
        opportunity (dict): Must have keys: content_category, device_type, timestamp

    Returns:
        float: the computed score, or 0.0 if bid_amount is missing/invalid
    """
    bid_amount = bid.get("bid_amount", 0.0)
    if not isinstance(bid_amount, (int, float)) or bid_amount <= 0:
        return 0.0

    # Relevance multiplier
    content_cat = opportunity.get("content_category", "")
    ad_cat = bid.get("category", "")
    relevance = RELEVANCE_MAP.get((content_cat, ad_cat), 1.0)

    # Time bonus - extract hour from ISO 8601 timestamp
    time_bonus = 1.0
    try:
        ts_str = opportunity.get("timestamp", "")
        hour = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).hour
        for start_h, end_h, bonus in TIME_WINDOWS:
            if start_h <= hour < end_h:
                time_bonus = bonus
                break
    except (ValueError, AttributeError):
        pass  # If timestamp is malformed, no time bonus

    # Device bonus
    device = opportunity.get("device_type", "desktop")
    device_bonus = DEVICE_BONUS.get(device, 1.0)

    return bid_amount * relevance * time_bonus * device_bonus


# ---------------------------------------------------------------------------
# Winner selection
# ---------------------------------------------------------------------------

def select_winner(opportunity):
    """
    Evaluate all bids for a single opportunity and return the winner.

    Args:
        opportunity (dict): Full opportunity dict including 'bids' list.

    Returns:
        dict with keys: winning_advertiser_id, winning_bid_amount,
                        winning_score, score_margin
        None if no valid bids exist.
    """
    bids = opportunity.get("bids", [])
    if not bids:
        return None

    scored = []
    for bid in bids:
        score = compute_score(bid, opportunity)
        scored.append((score, bid))

    scored.sort(key=lambda x: x[0], reverse=True)

    winner_score, winner_bid = scored[0]
    second_score = scored[1][0] if len(scored) > 1 else 0.0

    return {
        "winning_advertiser_id": winner_bid["advertiser_id"],
        "winning_bid_amount": winner_bid["bid_amount"],
        "winning_score": round(winner_score, 4),
        "score_margin": round(winner_score - second_score, 4),
    }


# ---------------------------------------------------------------------------
# Batch processing
# ---------------------------------------------------------------------------

def process_opportunity(opportunity):
    """
    Process a single opportunity end-to-end:
    1. Score all bids and select winner
    2. Post result to SQS results queue (fast path)
    3. Persist result to DynamoDB (storage path)

    Args:
        opportunity (dict): A single ad opportunity message.

    Returns:
        dict: The result record that was posted.
    """
    msg_start = time.perf_counter()

    result = select_winner(opportunity)
    if result is None:
        logger.warning(
            "No valid bids for opportunity %s",
            opportunity.get("opportunity_id", "unknown"),
        )
        return None

    processed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    result_record = {
        "opportunity_id": opportunity["opportunity_id"],
        "winning_advertiser_id": result["winning_advertiser_id"],
        "winning_bid_amount": result["winning_bid_amount"],
        "winning_score": result["winning_score"],
        "score_margin": result["score_margin"],
        "processed_at": processed_at,
    }

    # Step 1: Post to results queue FIRST - this is the latency-measured path
    sqs.send_message(
        QueueUrl=RESULTS_QUEUE_URL,
        MessageBody=json.dumps(result_record),
    )

    # Step 2: Persist to DynamoDB - storage, tolerates more latency
    table = dynamodb.Table(DYNAMO_TABLE_NAME)
    table.put_item(
        Item={
            "opportunity_id": result_record["opportunity_id"],
            "winning_advertiser_id": result_record["winning_advertiser_id"],
            "winning_bid_amount": Decimal(str(result_record["winning_bid_amount"])),
            "winning_score": Decimal(str(result_record["winning_score"])),
            "score_margin": Decimal(str(result_record["score_margin"])),
            "processed_at": result_record["processed_at"],
        }
    )

    msg_elapsed = (time.perf_counter() - msg_start) * 1000
    logger.info(
        "Processed %s in %.1f ms | winner=%s score=%.4f margin=%.4f",
        opportunity["opportunity_id"],
        msg_elapsed,
        result["winning_advertiser_id"],
        result["winning_score"],
        result["score_margin"],
    )

    return result_record


# ---------------------------------------------------------------------------
# Lambda entry point
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    """
    Lambda entry point. Receives a batch of SQS messages, processes each
    opportunity, and returns partial batch failure information.

    The batchItemFailures response format tells SQS which specific messages
    failed so only those are retried. Without this, a single failure causes
    the entire batch to retry.

    Viewing logs after deployment:
        aws logs tail /aws/lambda/adflow-YOURID-worker --follow
        aws logs filter-log-events \\
            --log-group-name /aws/lambda/adflow-YOURID-worker \\
            --filter-pattern "Batch complete"

    Args:
        event (dict): SQS event with 'Records' list.
        context: Lambda context (has aws_request_id, function_name, etc.)

    Returns:
        dict: {"batchItemFailures": [{"itemIdentifier": "msg_id"}, ...]}
    """
    batch_start = time.perf_counter()
    records = event.get("Records", [])
    request_id = getattr(context, "aws_request_id", "local") if context else "local"

    logger.info(
        "Batch start: %d messages | request_id=%s",
        len(records),
        request_id,
    )

    failed_ids = []
    success_count = 0

    for record in records:
        try:
            opportunity = json.loads(record["body"])
            result = process_opportunity(opportunity)
            if result is not None:
                success_count += 1
        except Exception:
            msg_id = record.get("messageId", "unknown")
            logger.error(
                "Failed message %s", msg_id, exc_info=True,
            )
            failed_ids.append(msg_id)

    batch_elapsed = (time.perf_counter() - batch_start) * 1000
    logger.info(
        "Batch complete: %d/%d succeeded in %.1f ms | failures=%d | request_id=%s",
        success_count,
        len(records),
        batch_elapsed,
        len(failed_ids),
        request_id,
    )

    return {
        "batchItemFailures": [
            {"itemIdentifier": mid} for mid in failed_ids
        ]
    }
