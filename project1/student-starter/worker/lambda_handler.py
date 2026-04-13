"""
AdFlow Lambda Bid Engine
========================
Processes SQS batches of ad auction opportunities, selects a winner using
quality-adjusted scoring, posts results to a results queue, and persists
each record to DynamoDB.

Expected log format (Task 4):
    Batch complete: {success}/{total} succeeded in {elapsed:.1f} ms
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
# AWS clients  (module-level — reused across warm Lambda invocations)
# ---------------------------------------------------------------------------
sqs = boto3.client("sqs")
dynamodb = boto3.resource("dynamodb")

RESULTS_QUEUE_URL = os.environ["RESULTS_QUEUE_URL"]
DYNAMO_TABLE_NAME = os.environ["DYNAMO_TABLE_NAME"]
table = dynamodb.Table(DYNAMO_TABLE_NAME)

# ---------------------------------------------------------------------------
# Scoring constants
# ---------------------------------------------------------------------------

# (content_category, advertiser_category) -> relevance multiplier
RELEVANCE_MAP = {
    ("sports",        "sportswear"):  1.4,
    ("sports",        "energy_drink"):1.3,
    ("finance",       "fintech"):     1.5,
    ("finance",       "insurance"):   1.3,
    ("entertainment", "streaming"):   1.4,
    ("entertainment", "gaming"):      1.3,
    ("lifestyle",     "beauty"):      1.3,
    ("lifestyle",     "travel"):      1.2,
}

# (start_hour_inclusive, end_hour_inclusive, bonus)  — UTC
TIME_WINDOWS = [
    (6,  8,  1.20),   # morning commute
    (12, 13, 1.15),   # lunch
    (19, 22, 1.25),   # evening peak
]

DEVICE_BONUS = {
    "mobile":  1.1,
    "desktop": 1.0,
}


# ---------------------------------------------------------------------------
# Task 1 — compute_score
# ---------------------------------------------------------------------------

def compute_score(bid: dict, opportunity: dict) -> float:
    """Return the quality-adjusted score for one bid given an opportunity.

    Formula:
        score = bid_amount x relevance_multiplier x time_bonus x device_bonus

    Edge cases handled:
        - bid_amount missing or zero   -> return 0.0
        - category pair not in map     -> relevance_multiplier = 1.0
        - timestamp unparseable        -> time_bonus = 1.0 (no adjustment)
    """
    # --- bid_amount ---
    bid_amount = bid.get("bid_amount", 0)
    try:
        bid_amount = float(bid_amount)
    except (TypeError, ValueError):
        bid_amount = 0.0

    if bid_amount <= 0:
        return 0.0

    # --- relevance multiplier ---
    content_cat    = opportunity.get("content_category", "")
    advertiser_cat = bid.get("category", "")
    relevance      = RELEVANCE_MAP.get((content_cat, advertiser_cat), 1.0)

    # --- time bonus ---
    time_bonus = 1.0
    try:
        raw_ts = opportunity.get("timestamp", "")
        ts     = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
        hour   = ts.astimezone(timezone.utc).hour
        for start, end, bonus in TIME_WINDOWS:
            if start <= hour <= end:
                time_bonus = bonus
                break
    except (ValueError, AttributeError):
        time_bonus = 1.0  # unparseable timestamp -> no adjustment

    # --- device bonus ---
    device_type  = opportunity.get("device_type", "desktop")
    device_bonus = DEVICE_BONUS.get(device_type, 1.0)

    return bid_amount * relevance * time_bonus * device_bonus


# ---------------------------------------------------------------------------
# Task 2 — select_winner
# ---------------------------------------------------------------------------

def select_winner(opportunity: dict) -> dict | None:
    """Evaluate all bids and return the winner, or None if none score > 0.

    Returns a dict with keys:
        winning_advertiser_id, winning_bid_amount, winning_score, score_margin
    """
    bids = opportunity.get("bids", [])
    if not bids:
        return None

    scored = sorted(
        [(compute_score(bid, opportunity), bid) for bid in bids],
        key=lambda x: x[0],
        reverse=True,
    )

    best_score, best_bid = scored[0]
    if best_score <= 0:
        return None

    second_score = scored[1][0] if len(scored) > 1 else 0.0

    return {
        "winning_advertiser_id": best_bid["advertiser_id"],
        "winning_bid_amount":    float(best_bid["bid_amount"]),
        "winning_score":         float(best_score),
        "score_margin":          float(best_score - second_score),
    }


# ---------------------------------------------------------------------------
# Task 3 — process_opportunity
# ---------------------------------------------------------------------------

def process_opportunity(opportunity: dict) -> None:
    """Process one opportunity end-to-end.

    1. Select winner via select_winner().
    2. Build the result record.
    3. Post to RESULTS_QUEUE_URL (latency-critical — SQS first).
    4. Persist to DynamoDB with Decimal-wrapped numerics.
    """
    winner = select_winner(opportunity)
    if winner is None:
        logger.warning(
            "No valid winner for opportunity %s — skipping.",
            opportunity.get("opportunity_id"),
        )
        return

    processed_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    result = {
        "opportunity_id":        opportunity["opportunity_id"],
        "winning_advertiser_id": winner["winning_advertiser_id"],
        "winning_bid_amount":    winner["winning_bid_amount"],
        "winning_score":         winner["winning_score"],
        "score_margin":          winner["score_margin"],
        "processed_at":          processed_at,
        "content_category":      opportunity.get("content_category", ""),
    }

    # Step 3 — SQS (standard Python floats are fine for JSON serialisation)
    sqs.send_message(
        QueueUrl=RESULTS_QUEUE_URL,
        MessageBody=json.dumps(result),
    )

    # Step 4 — DynamoDB (float not accepted; must use Decimal)
    dynamo_item = {
        "opportunity_id":        result["opportunity_id"],
        "winning_advertiser_id": result["winning_advertiser_id"],
        "winning_bid_amount":    Decimal(str(result["winning_bid_amount"])),
        "winning_score":         Decimal(str(result["winning_score"])),
        "score_margin":          Decimal(str(result["score_margin"])),
        "processed_at":          result["processed_at"],
        "content_category":      result["content_category"],
    }
    table.put_item(Item=dynamo_item)


# ---------------------------------------------------------------------------
# Task 4 — lambda_handler
# ---------------------------------------------------------------------------

def lambda_handler(event: dict, context) -> dict:
    """Lambda entry point — processes a batch of SQS records.

    Each record is parsed and processed independently.  Failures are
    collected and returned as batchItemFailures so SQS retries only the
    failed messages rather than the entire batch.
    """
    records    = event.get("Records", [])
    total      = len(records)
    batch_start = time.perf_counter()

    batch_item_failures: list[dict] = []
    success_count = 0

    for record in records:
        message_id = record["messageId"]
        msg_start  = time.perf_counter()

        # --- Parse JSON body ---
        try:
            opportunity = json.loads(record["body"])
        except json.JSONDecodeError as exc:
            logger.error("JSONDecodeError messageId=%s: %s", message_id, exc)
            batch_item_failures.append({"itemIdentifier": message_id})
            continue

        # --- Process ---
        try:
            process_opportunity(opportunity)
            elapsed_ms = (time.perf_counter() - msg_start) * 1000
            logger.info(
                "Processed %s in %.1f ms",
                opportunity.get("opportunity_id", message_id),
                elapsed_ms,
            )
            success_count += 1
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Error processing messageId=%s opportunity=%s: %s",
                message_id,
                opportunity.get("opportunity_id", "unknown"),
                exc,
                exc_info=True,
            )
            batch_item_failures.append({"itemIdentifier": message_id})

    batch_elapsed_ms = (time.perf_counter() - batch_start) * 1000
    logger.info(
        "Batch complete: %d/%d succeeded in %.1f ms",
        success_count,
        total,
        batch_elapsed_ms,
    )

    return {"batchItemFailures": batch_item_failures}
