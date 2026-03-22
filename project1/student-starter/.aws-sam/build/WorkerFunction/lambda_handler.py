"""
AdFlow Ad Selection Worker
===========================

This Lambda function processes ad opportunity messages from an SQS queue,
applies a quality-adjusted scoring function to select winning bids,
and posts results onward.

Your Tasks:
    1. implement compute_score()    - the scoring formula
    2. implement select_winner()    - pick the winning bid
    3. implement process_opportunity() - full message processing
    4. implement lambda_handler()   - batch processing with failure handling

Logging and Performance:
    Use the logger for all output (not print). Example:
        logger.info("Processed %s in %.1f ms", opportunity_id, elapsed_ms)

    Measure wall-clock time for performance:
        start = time.perf_counter()
        # ... do work ...
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info("Operation took %.1f ms", elapsed_ms)

    To view your logs after deployment:
        aws logs tail /aws/lambda/adflow-YOURID-worker --follow

    To search logs for specific patterns:
        aws logs filter-log-events \
            --log-group-name /aws/lambda/adflow-YOURID-worker \
            --filter-pattern "Batch complete"

    In the AWS Console:
        CloudWatch > Log groups > /aws/lambda/adflow-YOURID-worker
"""

import json
import os
import time
import logging
from datetime import datetime, timezone
from decimal import Decimal

import boto3


# ---------------------------------------------------------------------------
# Logging - CloudWatch picks up anything written to the logger
# ---------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# AWS clients - created once per cold start, reused across invocations
# ---------------------------------------------------------------------------
sqs = boto3.client("sqs")
dynamodb = boto3.resource("dynamodb")

RESULTS_QUEUE_URL = os.environ.get("RESULTS_QUEUE_URL", "")
DYNAMO_TABLE_NAME = os.environ.get("DYNAMO_TABLE_NAME", "")

# ---------------------------------------------------------------------------
# Scoring constants - from the assignment specification
# ---------------------------------------------------------------------------

# Relevance multiplier: (content_category, advertiser_category) -> multiplier
# Any combination not listed here receives 1.0
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

# Device bonus
DEVICE_BONUS = {
    "mobile": 1.1,
    "desktop": 1.0,
}


# ---------------------------------------------------------------------------
# Task 1: Scoring Function
# ---------------------------------------------------------------------------

def compute_score(bid, opportunity):
    """
    Compute the quality-adjusted score for a single bid.

    Formula:
        score = bid_amount * relevance_multiplier * time_bonus * device_bonus

    Args:
        bid (dict): Keys: advertiser_id, bid_amount, category
        opportunity (dict): Keys: content_category, device_type, timestamp

    Returns:
        float: the computed score
    """
    # 1. Handle edge case: missing or zero bid_amount
    bid_amount = bid.get("bid_amount")
    if bid_amount is None or bid_amount <= 0:
        return 0.0

    # 2. Relevance Multiplier
    # Default to 1.0 if the specific (content, ad) combination is not in the map
    content_category = opportunity.get("content_category")
    advertiser_category = bid.get("category")
    relevance_multiplier = RELEVANCE_MAP.get((content_category, advertiser_category), 1.0)

    # 3. Time Bonus
    # Parse timestamp and find matching hour window. Default to 1.0 if parsing fails.
    time_bonus = 1.0
    timestamp_str = opportunity.get("timestamp")
    if timestamp_str:
        try:
            # Replace 'Z' with '+00:00' to ensure fromisoformat parses correctly safely
            dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            hour = dt.hour
            
            # Check which time window the hour falls into
            for start_h, end_h, bonus in TIME_WINDOWS:
                if start_h <= hour < end_h:
                    time_bonus = bonus
                    break
        except (ValueError, TypeError):
            # If timestamp is malformed or not a string, we keep time_bonus at 1.0
            pass

    # 4. Device Bonus
    # Default to 1.0 if device is unknown or missing
    device_type = opportunity.get("device_type")
    device_bonus = DEVICE_BONUS.get(device_type, 1.0)

    # 5. Calculate and return final score
    return float(bid_amount * relevance_multiplier * time_bonus * device_bonus)


# ---------------------------------------------------------------------------
# Task 2: Winner Selection
# ---------------------------------------------------------------------------

def select_winner(opportunity):
    """
    Evaluate all bids for an opportunity and return the winner.

    Args:
        opportunity (dict): Full opportunity including 'bids' list.

    Returns:
        dict with keys:
            winning_advertiser_id (str)
            winning_bid_amount (float)
            winning_score (float)
            score_margin (float) - winning score minus second-place score
        Returns None if there are no valid bids.
    """
    bids = opportunity.get("bids", [])
    
    # Edge case: Return None if the bids list is empty
    if not bids:
        return None

    best_score = 0.0
    second_best_score = 0.0
    winning_bid = None

    # Iterate through all bids to find the highest and second-highest scores
    for bid in bids:
        score = compute_score(bid, opportunity)
        
        if score > best_score:
            # New highest score found; current best drops to second best
            second_best_score = best_score
            best_score = score
            winning_bid = bid
        elif score > second_best_score:
            # Score doesn't beat the best, but beats the current second best
            second_best_score = score

    # Edge case: Return None if all bids score zero (or lower)
    if best_score <= 0.0 or winning_bid is None:
        return None

    # Calculate the margin between the winner and the runner-up
    score_margin = best_score - second_best_score

    # Return the required dictionary
    return {
        "winning_advertiser_id": winning_bid["advertiser_id"],
        "winning_bid_amount": winning_bid.get("bid_amount", 0.0),
        "winning_score": best_score,
        "score_margin": score_margin
    }


# ---------------------------------------------------------------------------
# Task 3: Process a Single Opportunity
# ---------------------------------------------------------------------------

def process_opportunity(opportunity):
    """
    Process one opportunity end-to-end:
        1. Select the winning bid
        2. Construct the result record
        3. Send the result where it needs to go
    """
    # 1. Select the winning bid
    winner_info = select_winner(opportunity)
    
    # If no valid bids were found, there is nothing to process
    if not winner_info:
        return None

    # 2. Build the result record
    # Generate ISO 8601 timestamp in UTC (appending 'Z' for strict compliance)
    processed_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    result_record = {
        "opportunity_id": opportunity["opportunity_id"],
        "winning_advertiser_id": winner_info["winning_advertiser_id"],
        "winning_bid_amount": winner_info["winning_bid_amount"],
        "winning_score": winner_info["winning_score"],
        "score_margin": winner_info["score_margin"],
        "processed_at": processed_at,
        "content_category": opportunity.get("content_category", "unknown")
    }

    # 3. Post the result to SQS FIRST (Latency-sensitive path)
    # json.dumps handles standard Python floats perfectly
    sqs.send_message(
        QueueUrl=RESULTS_QUEUE_URL,
        MessageBody=json.dumps(result_record)
    )

    # 4. Write the record to DynamoDB SECOND (Persistence path)
    # DynamoDB requires Python floats to be wrapped in Decimal(str(value))
    dynamo_record = {
        "opportunity_id": result_record["opportunity_id"],
        "winning_advertiser_id": result_record["winning_advertiser_id"],
        "winning_bid_amount": Decimal(str(result_record["winning_bid_amount"])),
        "winning_score": Decimal(str(result_record["winning_score"])),
        "score_margin": Decimal(str(result_record["score_margin"])),
        "processed_at": result_record["processed_at"],
        "content_category": result_record["content_category"]
    }

    table = dynamodb.Table(DYNAMO_TABLE_NAME)
    table.put_item(Item=dynamo_record)

    # Return the original dict (with standard floats) for testing/logging
    return result_record


# ---------------------------------------------------------------------------
# Task 4: Lambda Entry Point with Batch Processing
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    """
    Lambda entry point. Receives a batch of SQS messages.
    """
    # Start the timer for the entire batch
    start_time = time.perf_counter()
    
    failed_messages = []
    records = event.get("Records", [])
    
    for record in records:
        message_id = record.get("messageId")
        
        try:
            # 1. Parse the JSON body
            body_str = record.get("body", "{}")
            opportunity = json.loads(body_str)
            
            # 2. Process the opportunity end-to-end
            process_opportunity(opportunity)
            
        except json.JSONDecodeError as e:
            # Catch malformed JSON specifically
            logger.error("JSONDecodeError for message %s: %s", message_id, str(e))
            failed_messages.append({"itemIdentifier": message_id})
            
        except Exception as e:
            # Catch any other processing errors (e.g., missing keys, DynamoDB timeout)
            logger.error("Error processing message %s: %s", message_id, str(e))
            failed_messages.append({"itemIdentifier": message_id})
            
    # Calculate elapsed time in milliseconds
    elapsed_ms = (time.perf_counter() - start_time) * 1000
    
    # Log the batch timing format expected by the instructions
    successful_count = len(records) - len(failed_messages)
    logger.info("Batch complete: %d/%d succeeded in %.1f ms", successful_count, len(records), elapsed_ms)

    # Return exactly this format so SQS handles retries properly
    return {
        "batchItemFailures": failed_messages
    }
