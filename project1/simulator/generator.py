"""
AdFlow Opportunity Stream Simulator
====================================

Generates realistic ad opportunity data for testing the bid selection pipeline.
Can be used as a Python module or run from the command line.

Usage:
    from simulator.generator import generate_opportunities
    opportunities = generate_opportunities(count=500, seed=42)

    python generator.py --count 500 --seed 42 --output opportunities.json
"""

import json
import uuid
import random
from datetime import datetime, timedelta, timezone
import argparse
import sys


# ---------------------------------------------------------------------------
# Advertiser pool - each has an ID, category, and typical bid range (USD CPM)
# ---------------------------------------------------------------------------
ADVERTISERS = [
    {"advertiser_id": "adv_sportswear_01", "category": "sportswear", "bid_min": 2.00, "bid_max": 5.50},
    {"advertiser_id": "adv_sportswear_02", "category": "sportswear", "bid_min": 1.50, "bid_max": 4.00},
    {"advertiser_id": "adv_energy_01", "category": "energy_drink", "bid_min": 2.50, "bid_max": 6.00},
    {"advertiser_id": "adv_energy_02", "category": "energy_drink", "bid_min": 1.80, "bid_max": 4.50},
    {"advertiser_id": "adv_fintech_01", "category": "fintech", "bid_min": 3.00, "bid_max": 8.00},
    {"advertiser_id": "adv_fintech_02", "category": "fintech", "bid_min": 2.00, "bid_max": 5.00},
    {"advertiser_id": "adv_insurance_01", "category": "insurance", "bid_min": 3.50, "bid_max": 7.00},
    {"advertiser_id": "adv_insurance_02", "category": "insurance", "bid_min": 2.00, "bid_max": 5.50},
    {"advertiser_id": "adv_streaming_01", "category": "streaming", "bid_min": 2.50, "bid_max": 6.50},
    {"advertiser_id": "adv_streaming_02", "category": "streaming", "bid_min": 1.50, "bid_max": 4.00},
    {"advertiser_id": "adv_gaming_01", "category": "gaming", "bid_min": 2.00, "bid_max": 5.00},
    {"advertiser_id": "adv_gaming_02", "category": "gaming", "bid_min": 1.00, "bid_max": 3.50},
    {"advertiser_id": "adv_beauty_01", "category": "beauty", "bid_min": 2.50, "bid_max": 5.50},
    {"advertiser_id": "adv_beauty_02", "category": "beauty", "bid_min": 1.50, "bid_max": 4.00},
    {"advertiser_id": "adv_travel_01", "category": "travel", "bid_min": 3.00, "bid_max": 7.50},
    {"advertiser_id": "adv_travel_02", "category": "travel", "bid_min": 2.00, "bid_max": 5.00},
    {"advertiser_id": "adv_fastfood_01", "category": "fast_food", "bid_min": 3.50, "bid_max": 7.00},
    {"advertiser_id": "adv_fastfood_02", "category": "fast_food", "bid_min": 2.50, "bid_max": 6.00},
    {"advertiser_id": "adv_auto_01", "category": "automotive", "bid_min": 4.00, "bid_max": 8.00},
    {"advertiser_id": "adv_auto_02", "category": "automotive", "bid_min": 3.00, "bid_max": 6.50},
    {"advertiser_id": "adv_telecom_01", "category": "telecom", "bid_min": 3.00, "bid_max": 6.00},
    {"advertiser_id": "adv_retail_01", "category": "retail", "bid_min": 1.50, "bid_max": 4.50},
    {"advertiser_id": "adv_retail_02", "category": "retail", "bid_min": 1.00, "bid_max": 3.00},
]

CONTENT_CATEGORIES = ["sports", "news", "entertainment", "finance", "lifestyle"]

# Weights for content categories (sports and entertainment slightly more common)
CONTENT_WEIGHTS = [0.25, 0.15, 0.25, 0.20, 0.15]

DEVICES = ["mobile", "desktop"]
DEVICE_WEIGHTS = [0.70, 0.30]  # Mobile-heavy ad market

REGIONS = ["northeast", "southeast", "midwest", "west", "international"]
REGION_WEIGHTS = [0.20, 0.25, 0.20, 0.25, 0.10]

# Hour distribution weights -- heavier during waking/peak hours
# Index = UTC hour, value = relative weight
HOUR_WEIGHTS = [
    0.5, 0.3, 0.2, 0.2, 0.3, 0.5,   # 00-05: overnight low
    1.5, 2.0, 2.0, 1.5, 1.2, 1.0,   # 06-11: morning ramp
    1.8, 1.8, 1.2, 1.0, 1.0, 1.2,   # 12-17: afternoon
    1.5, 2.0, 2.5, 2.5, 2.0, 1.0,   # 18-23: evening peak
]


def _weighted_choice(rng, items, weights):
    """Select a single item using weighted random choice."""
    total = sum(weights)
    r = rng.random() * total
    cumulative = 0.0
    for item, weight in zip(items, weights):
        cumulative += weight
        if r <= cumulative:
            return item
    return items[-1]


def _generate_timestamp(rng, base_date=None):
    """Generate a timestamp weighted toward peak hours within a 24h window."""
    if base_date is None:
        base_date = datetime(2025, 3, 10, tzinfo=timezone.utc)

    hour = _weighted_choice(rng, list(range(24)), HOUR_WEIGHTS)
    minute = rng.randint(0, 59)
    second = rng.randint(0, 59)

    ts = base_date.replace(hour=hour, minute=minute, second=second)
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def _select_bidders(rng, count):
    """Select a set of unique advertisers to bid on this opportunity."""
    return rng.sample(ADVERTISERS, min(count, len(ADVERTISERS)))


def _generate_bid(rng, advertiser):
    """Generate a bid for a given advertiser within their typical range."""
    amount = round(rng.uniform(advertiser["bid_min"], advertiser["bid_max"]), 2)
    return {
        "advertiser_id": advertiser["advertiser_id"],
        "bid_amount": amount,
        "category": advertiser["category"],
    }


def generate_opportunity(rng, base_date=None):
    """
    Generate a single ad opportunity with competing bids.

    Args:
        rng: random.Random instance for reproducibility
        base_date: datetime for the timestamp base (default: 2025-03-10 UTC)

    Returns:
        dict: a complete opportunity message
    """
    num_bids = rng.randint(3, 8)
    bidders = _select_bidders(rng, num_bids)

    return {
        "opportunity_id": str(uuid.UUID(int=rng.getrandbits(128))),
        "timestamp": _generate_timestamp(rng, base_date),
        "user_id": f"u_{rng.randint(10000, 99999)}",
        "content_category": _weighted_choice(rng, CONTENT_CATEGORIES, CONTENT_WEIGHTS),
        "device_type": _weighted_choice(rng, DEVICES, DEVICE_WEIGHTS),
        "region": _weighted_choice(rng, REGIONS, REGION_WEIGHTS),
        "bids": [_generate_bid(rng, adv) for adv in bidders],
    }


def generate_opportunities(count=500, seed=42, base_date=None):
    """
    Generate a batch of ad opportunity messages.

    Args:
        count: number of opportunities to generate
        seed: random seed for reproducibility
        base_date: datetime for timestamp base (default: 2025-03-10 UTC)

    Returns:
        list[dict]: list of opportunity messages
    """
    rng = random.Random(seed)
    return [generate_opportunity(rng, base_date) for _ in range(count)]


def main():
    """CLI entry point for generating opportunity data."""
    parser = argparse.ArgumentParser(
        description="Generate AdFlow ad opportunity messages"
    )
    parser.add_argument(
        "--count", type=int, default=500,
        help="Number of opportunities to generate (default: 500)"
    )
    parser.add_argument(
        "--seed", type=int, default=42,
        help="Random seed for reproducibility (default: 42)"
    )
    parser.add_argument(
        "--output", type=str, default=None,
        help="Output file path (default: stdout)"
    )
    parser.add_argument(
        "--pretty", action="store_true",
        help="Pretty-print the JSON output"
    )
    args = parser.parse_args()

    opportunities = generate_opportunities(count=args.count, seed=args.seed)

    indent = 2 if args.pretty else None
    output = json.dumps(opportunities, indent=indent)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(output)
        print(f"Wrote {len(opportunities)} opportunities to {args.output}", file=sys.stderr)
    else:
        print(output)


if __name__ == "__main__":
    main()
