"""
AdFlow Ad Selection Worker — lambda_handler.py

Processes ad auction opportunities from SQS, scores bids using a
quality-adjusted formula, selects a winner, posts the result to the
results queue, and persists it to DynamoDB.

Expected log output per batch:
    Batch complete: 10/10 succeeded in 142.3 ms
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from decimal import Decimal

import boto3

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# AWS clients (initialised once per container — reused across invocations)
# ---------------------------------------------------------------------------
sqs = boto3.client("sqs")
dynamodb = boto3.resource("dynamodb")

RESULTS_QUEUE_URL = os.environ.get("RESULTS_QUEUE_URL", "")
DYNAMO_TABLE_NAME = os.environ.get("DYNAMO_TABLE_NAME", "")
table = dynamodb.Table(DYNAMO_TABLE_NAME) if DYNAMO_TABLE_NAME else None

# ---------------------------------------------------------------------------
# Scoring constants
# ---------------------------------------------------------------------------
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

TIME_WINDOWS = [
    (6, 9, 1.20),    # 06:00-08:59 morning commute
    (12, 14, 1.15),  # 12:00-13:59 lunch browsing
    (19, 23, 1.25),  # 19:00-22:59 evening peak
]

DEVICE_BONUS = {
    "mobile": 1.1,
    "desktop": 1.0,
}


# ---------------------------------------------------------------------------
# Task 1: compute_score
# ---------------------------------------------------------------------------
def compute_score(bid, opportunity):
    """
    Compute the quality-adjusted score for a single bid.

    score = bid_amount * relevance_multiplier * time_bonus * device_bonus

    Parameters
    ----------
    bid : dict
        One bid object with keys: advertiser_id, bid_amount, category
    opportunity : dict
        The full opportunity with keys like content_category, device_type,
        timestamp, etc.

    Returns
    -------
    float
        The computed score. Returns 0.0 for invalid/missing data.
    """
    # Get bid amount, default to 0 if missing or not a number
    bid_amount = bid.get("bid_amount", 0)
    if not isinstance(bid_amount, (int, float)):
        return 0.0
    if bid_amount <= 0:
        return 0.0

    # Relevance multiplier
    content_cat = opportunity.get("content_category", "")
    bid_cat = bid.get("category", "")
    relevance = RELEVANCE_MAP.get((content_cat, bid_cat), 1.0)

    # Time bonus — parse the timestamp hour
    time_bonus = 1.0
    timestamp_str = opportunity.get("timestamp", "")
    try:
        dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        hour = dt.hour
        for start, end, bonus in TIME_WINDOWS:
            if start <= hour < end:
                time_bonus = bonus
                break
    except (ValueError, AttributeError):
        # Bad or missing timestamp — no bonus, just use 1.0
        time_bonus = 1.0

    # Device bonus
    device = opportunity.get("device_type", "desktop")
    device_bonus = DEVICE_BONUS.get(device, 1.0)

    score = bid_amount * relevance * time_bonus * device_bonus
    return score


# ---------------------------------------------------------------------------
# Task 2: select_winner
# ---------------------------------------------------------------------------
def select_winner(opportunity):
    """
    Evaluate all bids for one opportunity and pick the winner.

    Parameters
    ----------
    opportunity : dict
        Full opportunity message including the 'bids' list.

    Returns
    -------
    dict or None
        Dict with winning_advertiser_id, winning_bid_amount,
        winning_score, score_margin.  None if no valid bids.
    """
    bids = opportunity.get("bids", [])
    if not bids:
        return None

    # Score every bid
    scored = []
    for bid in bids:
        s = compute_score(bid, opportunity)
        scored.append((s, bid))

    # Sort descending by score
    scored.sort(key=lambda x: x[0], reverse=True)

    # Check if all scores are zero
    if scored[0][0] == 0:
        return None

    best_score, best_bid = scored[0]
    second_score = scored[1][0] if len(scored) > 1 else 0.0

    return {
        "winning_advertiser_id": best_bid["advertiser_id"],
        "winning_bid_amount": best_bid["bid_amount"],
        "winning_score": best_score,
        "score_margin": best_score - second_score,
    }


# ---------------------------------------------------------------------------
# Task 3: process_opportunity
# ---------------------------------------------------------------------------
def process_opportunity(opportunity):
    """
    Process one opportunity end to end:
      1. Select the winner
      2. Build the result record
      3. Post it to SQS results queue
      4. Write it to DynamoDB

    Parameters
    ----------
    opportunity : dict
        The parsed opportunity message.

    Returns
    -------
    dict or None
        The result record, or None if no winner could be selected.
    """
    winner = select_winner(opportunity)
    if winner is None:
        return None

    processed_at = datetime.now(timezone.utc).isoformat()

    # Build result record
    result = {
        "opportunity_id": opportunity["opportunity_id"],
        "winning_advertiser_id": winner["winning_advertiser_id"],
        "winning_bid_amount": winner["winning_bid_amount"],
        "winning_score": winner["winning_score"],
        "score_margin": winner["score_margin"],
        "processed_at": processed_at,
        "content_category": opportunity.get("content_category", ""),
    }

    # Post to SQS first (latency-measured path)
    sqs.send_message(
        QueueUrl=RESULTS_QUEUE_URL,
        MessageBody=json.dumps(result),
    )

    # Write to DynamoDB (convert floats to Decimal)
    dynamo_item = {
        "opportunity_id": result["opportunity_id"],
        "winning_advertiser_id": result["winning_advertiser_id"],
        "winning_bid_amount": Decimal(str(result["winning_bid_amount"])),
        "winning_score": Decimal(str(result["winning_score"])),
        "score_margin": Decimal(str(result["score_margin"])),
        "processed_at": result["processed_at"],
        "content_category": result["content_category"],
    }
    ddb_table = dynamodb.Table(DYNAMO_TABLE_NAME)
    ddb_table.put_item(Item=dynamo_item)

    return result


# ---------------------------------------------------------------------------
# Task 4: lambda_handler
# ---------------------------------------------------------------------------
def lambda_handler(event, context):
    """
    Lambda entry point. Processes a batch of SQS records.

    Catches exceptions per-record so one bad message doesn't kill the batch.
    Returns batchItemFailures so SQS only retries the failed ones.

    Parameters
    ----------
    event : dict
        Lambda event with event["Records"] list of SQS messages.
    context : object
        Lambda context (unused).

    Returns
    -------
    dict
        {"batchItemFailures": [{"itemIdentifier": "<messageId>"}, ...]}
    """
    records = event.get("Records", [])
    failures = []
    succeeded = 0

    start = time.perf_counter()

    for record in records:
        msg_id = record.get("messageId", "unknown")
        try:
            opportunity = json.loads(record["body"])
            process_opportunity(opportunity)
            succeeded += 1
        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error for message {msg_id}: {e}")
            failures.append({"itemIdentifier": msg_id})
        except Exception as e:
            logger.error(f"Processing error for message {msg_id}: {e}")
            failures.append({"itemIdentifier": msg_id})

    elapsed_ms = (time.perf_counter() - start) * 1000
    total = len(records)
    logger.info(f"Batch complete: {succeeded}/{total} succeeded in {elapsed_ms:.1f} ms")

    return {"batchItemFailures": failures}
