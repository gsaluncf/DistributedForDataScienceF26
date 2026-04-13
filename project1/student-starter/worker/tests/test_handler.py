"""
test_handler.py
===============
Three test suites for the AdFlow Lambda bid engine.

  Suite 1 — Scoring accuracy      (Tasks 1)
  Suite 2 — Winner selection      (Task 2)
  Suite 3 — End-to-end batch      (Tasks 3 & 4)

Run with:
    pytest worker/tests/test_handler.py -v
"""

import json
import os
from decimal import Decimal
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_aws

# ---------------------------------------------------------------------------
# Environment stubs — must be set BEFORE importing lambda_handler
# ---------------------------------------------------------------------------
os.environ.setdefault("RESULTS_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123456789012/adflow-test-results")
os.environ.setdefault("DYNAMO_TABLE_NAME", "adflow-test-results")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_SECURITY_TOKEN", "testing")
os.environ.setdefault("AWS_SESSION_TOKEN", "testing")

from worker.lambda_handler import (  # noqa: E402
    compute_score,
    lambda_handler,
    process_opportunity,
    select_winner,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SPORTS_MOBILE_EVENING_OPPORTUNITY = {
    "opportunity_id": "opp-001",
    "timestamp": "2024-03-15T20:30:00Z",   # 20 UTC -> evening peak 1.25
    "user_id": "user-abc",
    "content_category": "sports",
    "device_type": "mobile",               # device bonus 1.1
    "region": "northeast",
    "bids": [
        {"advertiser_id": "adv_001", "bid_amount": 3.50, "category": "sportswear"},   # relevance 1.4
        {"advertiser_id": "adv_002", "bid_amount": 4.20, "category": "fast_food"},    # relevance 1.0
    ],
}

FINANCE_DESKTOP_MORNING_OPPORTUNITY = {
    "opportunity_id": "opp-002",
    "timestamp": "2024-03-15T07:00:00Z",   # 07 UTC -> morning commute 1.20
    "user_id": "user-xyz",
    "content_category": "finance",
    "device_type": "desktop",              # device bonus 1.0
    "region": "west",
    "bids": [
        {"advertiser_id": "adv_010", "bid_amount": 5.00, "category": "fintech"},      # relevance 1.5
        {"advertiser_id": "adv_011", "bid_amount": 6.00, "category": "fast_food"},    # relevance 1.0
    ],
}


# ===========================================================================
# Suite 1 — Scoring Accuracy
# ===========================================================================

class TestComputeScore:

    def test_worked_example_bid_a(self):
        """Worked example from spec: sports/mobile/evening, sportswear bid."""
        opp = SPORTS_MOBILE_EVENING_OPPORTUNITY
        bid = {"advertiser_id": "adv_001", "bid_amount": 3.50, "category": "sportswear"}
        score = compute_score(bid, opp)
        # 3.50 * 1.4 * 1.25 * 1.1
        assert abs(score - 6.7375) < 1e-6

    def test_worked_example_bid_b(self):
        """Worked example from spec: sports/mobile/evening, fast_food bid."""
        opp = SPORTS_MOBILE_EVENING_OPPORTUNITY
        bid = {"advertiser_id": "adv_002", "bid_amount": 4.20, "category": "fast_food"}
        score = compute_score(bid, opp)
        # 4.20 * 1.0 * 1.25 * 1.1
        assert abs(score - 5.775) < 1e-6

    def test_finance_fintech_morning_desktop(self):
        """finance/fintech with morning commute bonus and desktop device."""
        opp = FINANCE_DESKTOP_MORNING_OPPORTUNITY
        bid = {"advertiser_id": "adv_010", "bid_amount": 5.00, "category": "fintech"}
        score = compute_score(bid, opp)
        # 5.00 * 1.5 * 1.20 * 1.0
        assert abs(score - 9.0) < 1e-6

    def test_lunch_time_bonus(self):
        """Hour 12 UTC triggers lunch bonus 1.15."""
        opp = {
            "opportunity_id": "opp-lunch",
            "timestamp": "2024-03-15T12:30:00Z",
            "content_category": "news",
            "device_type": "desktop",
            "bids": [],
        }
        bid = {"advertiser_id": "adv_x", "bid_amount": 2.00, "category": "other"}
        score = compute_score(bid, opp)
        # 2.00 * 1.0 * 1.15 * 1.0
        assert abs(score - 2.30) < 1e-6

    def test_no_time_bonus_off_peak(self):
        """Hour 03 UTC has no time bonus (multiplier = 1.0)."""
        opp = {
            "opportunity_id": "opp-night",
            "timestamp": "2024-03-15T03:00:00Z",
            "content_category": "news",
            "device_type": "desktop",
            "bids": [],
        }
        bid = {"advertiser_id": "adv_x", "bid_amount": 2.00, "category": "other"}
        score = compute_score(bid, opp)
        assert abs(score - 2.00) < 1e-6

    def test_zero_bid_amount_returns_zero(self):
        """bid_amount == 0 must return 0.0."""
        opp = SPORTS_MOBILE_EVENING_OPPORTUNITY
        bid = {"advertiser_id": "adv_zero", "bid_amount": 0, "category": "sportswear"}
        assert compute_score(bid, opp) == 0.0

    def test_missing_bid_amount_returns_zero(self):
        """Missing bid_amount key must return 0.0."""
        opp = SPORTS_MOBILE_EVENING_OPPORTUNITY
        bid = {"advertiser_id": "adv_missing", "category": "sportswear"}
        assert compute_score(bid, opp) == 0.0

    def test_unknown_category_pair_uses_default_multiplier(self):
        """Category pair not in RELEVANCE_MAP -> multiplier 1.0."""
        opp = {
            "opportunity_id": "opp-x",
            "timestamp": "2024-03-15T03:00:00Z",
            "content_category": "sports",
            "device_type": "desktop",
            "bids": [],
        }
        bid = {"advertiser_id": "adv_x", "bid_amount": 3.00, "category": "furniture"}
        score = compute_score(bid, opp)
        # 3.00 * 1.0 * 1.0 * 1.0
        assert abs(score - 3.00) < 1e-6

    def test_malformed_timestamp_uses_no_time_bonus(self):
        """Unparseable timestamp -> time_bonus = 1.0, no crash."""
        opp = {
            "opportunity_id": "opp-bad-ts",
            "timestamp": "NOT-A-DATE",
            "content_category": "news",
            "device_type": "desktop",
            "bids": [],
        }
        bid = {"advertiser_id": "adv_x", "bid_amount": 4.00, "category": "other"}
        score = compute_score(bid, opp)
        assert abs(score - 4.00) < 1e-6


# ===========================================================================
# Suite 2 — Winner Selection
# ===========================================================================

class TestSelectWinner:

    def test_lower_bidder_wins_via_relevance(self):
        """adv_001 ($3.50 sportswear) should beat adv_002 ($4.20 fast_food)."""
        result = select_winner(SPORTS_MOBILE_EVENING_OPPORTUNITY)
        assert result is not None
        assert result["winning_advertiser_id"] == "adv_001"

    def test_winning_bid_amount_is_raw_not_scored(self):
        """winning_bid_amount must be the raw bid, not the adjusted score."""
        result = select_winner(SPORTS_MOBILE_EVENING_OPPORTUNITY)
        assert abs(result["winning_bid_amount"] - 3.50) < 1e-6

    def test_winning_score_is_adjusted(self):
        """winning_score must equal the full quality-adjusted score."""
        result = select_winner(SPORTS_MOBILE_EVENING_OPPORTUNITY)
        assert abs(result["winning_score"] - 6.7375) < 1e-6

    def test_score_margin_is_correct(self):
        """score_margin = winning_score - second_place_score."""
        result = select_winner(SPORTS_MOBILE_EVENING_OPPORTUNITY)
        expected_margin = 6.7375 - 5.775
        assert abs(result["score_margin"] - expected_margin) < 1e-6

    def test_empty_bids_returns_none(self):
        """No bids -> None."""
        opp = {**SPORTS_MOBILE_EVENING_OPPORTUNITY, "bids": []}
        assert select_winner(opp) is None

    def test_all_zero_bids_returns_none(self):
        """All bids score zero -> None."""
        opp = {
            **SPORTS_MOBILE_EVENING_OPPORTUNITY,
            "bids": [
                {"advertiser_id": "adv_z1", "bid_amount": 0, "category": "sportswear"},
                {"advertiser_id": "adv_z2", "bid_amount": 0, "category": "fast_food"},
            ],
        }
        assert select_winner(opp) is None

    def test_single_bid_margin_is_full_score(self):
        """With only one bid, second_score = 0, so margin == winning_score."""
        opp = {
            **SPORTS_MOBILE_EVENING_OPPORTUNITY,
            "bids": [
                {"advertiser_id": "adv_solo", "bid_amount": 3.00, "category": "energy_drink"},
            ],
        }
        result = select_winner(opp)
        assert result is not None
        assert abs(result["score_margin"] - result["winning_score"]) < 1e-6


# ===========================================================================
# Suite 3 — End-to-End Batch (moto mocks)
# ===========================================================================

def _make_sqs_record(opportunity: dict, message_id: str = "msg-001") -> dict:
    return {
        "messageId": message_id,
        "receiptHandle": "handle-" + message_id,
        "body": json.dumps(opportunity),
        "attributes": {},
        "messageAttributes": {},
        "md5OfBody": "abc",
        "eventSource": "aws:sqs",
        "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:adflow-test-input",
        "awsRegion": "us-east-1",
    }


@pytest.fixture()
def aws_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture()
def mock_aws_resources(aws_credentials):
    """Spin up moto SQS + DynamoDB and patch the module-level clients."""
    with mock_aws():
        # Create SQS queue
        sqs_client = boto3.client("sqs", region_name="us-east-1")
        queue = sqs_client.create_queue(QueueName="adflow-test-results")
        queue_url = queue["QueueUrl"]

        # Create DynamoDB table
        ddb = boto3.resource("dynamodb", region_name="us-east-1")
        ddb_table = ddb.create_table(
            TableName="adflow-test-results",
            AttributeDefinitions=[{"AttributeName": "opportunity_id", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "opportunity_id", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )

        import worker.lambda_handler as handler
        original_sqs   = handler.sqs
        original_table = handler.table

        handler.sqs   = sqs_client
        handler.table = ddb_table
        os.environ["RESULTS_QUEUE_URL"] = queue_url

        yield {"sqs": sqs_client, "queue_url": queue_url, "table": ddb_table}

        handler.sqs   = original_sqs
        handler.table = original_table


class TestEndToEnd:

    def test_batch_writes_to_sqs(self, mock_aws_resources):
        """A processed opportunity must produce a message in the results queue."""
        event = {"Records": [_make_sqs_record(SPORTS_MOBILE_EVENING_OPPORTUNITY, "msg-e2e-1")]}
        response = lambda_handler(event, None)

        assert response["batchItemFailures"] == []

        msgs = mock_aws_resources["sqs"].receive_message(
            QueueUrl=mock_aws_resources["queue_url"],
            MaxNumberOfMessages=10,
        ).get("Messages", [])
        assert len(msgs) == 1
        body = json.loads(msgs[0]["Body"])
        assert body["opportunity_id"] == "opp-001"
        assert body["winning_advertiser_id"] == "adv_001"

    def test_batch_writes_to_dynamodb(self, mock_aws_resources):
        """A processed opportunity must appear in DynamoDB."""
        event = {"Records": [_make_sqs_record(SPORTS_MOBILE_EVENING_OPPORTUNITY, "msg-e2e-2")]}
        lambda_handler(event, None)

        item = mock_aws_resources["table"].get_item(
            Key={"opportunity_id": "opp-001"}
        ).get("Item")
        assert item is not None
        assert item["winning_advertiser_id"] == "adv_001"
        assert isinstance(item["winning_bid_amount"], Decimal)
        assert isinstance(item["winning_score"], Decimal)

    def test_result_schema_includes_content_category(self, mock_aws_resources):
        """content_category must be present in both SQS and DynamoDB records."""
        event = {"Records": [_make_sqs_record(SPORTS_MOBILE_EVENING_OPPORTUNITY, "msg-e2e-3")]}
        lambda_handler(event, None)

        msgs = mock_aws_resources["sqs"].receive_message(
            QueueUrl=mock_aws_resources["queue_url"],
            MaxNumberOfMessages=10,
        ).get("Messages", [])
        body = json.loads(msgs[0]["Body"])
        assert body["content_category"] == "sports"

        item = mock_aws_resources["table"].get_item(
            Key={"opportunity_id": "opp-001"}
        ).get("Item")
        assert item["content_category"] == "sports"

    def test_result_schema_includes_processed_at(self, mock_aws_resources):
        """processed_at must be an ISO 8601 string in the result."""
        event = {"Records": [_make_sqs_record(SPORTS_MOBILE_EVENING_OPPORTUNITY, "msg-e2e-4")]}
        lambda_handler(event, None)

        msgs = mock_aws_resources["sqs"].receive_message(
            QueueUrl=mock_aws_resources["queue_url"],
            MaxNumberOfMessages=10,
        ).get("Messages", [])
        body = json.loads(msgs[0]["Body"])
        assert "processed_at" in body
        # Should be parseable as ISO 8601
        from datetime import datetime
        datetime.fromisoformat(body["processed_at"].replace("Z", "+00:00"))

    def test_malformed_json_isolated_does_not_abort_batch(self, mock_aws_resources):
        """A malformed JSON record must be reported as a failure but the rest processed."""
        good_record = _make_sqs_record(SPORTS_MOBILE_EVENING_OPPORTUNITY, "msg-good")
        bad_record  = {**_make_sqs_record({}, "msg-bad"), "body": "NOT JSON {{{"}

        event    = {"Records": [bad_record, good_record]}
        response = lambda_handler(event, None)

        # Bad record in failures
        failures = [f["itemIdentifier"] for f in response["batchItemFailures"]]
        assert "msg-bad" in failures
        assert "msg-good" not in failures

        # Good record still persisted
        item = mock_aws_resources["table"].get_item(
            Key={"opportunity_id": "opp-001"}
        ).get("Item")
        assert item is not None

    def test_batch_item_failures_format(self, mock_aws_resources):
        """batchItemFailures must use the exact key names SQS expects."""
        bad_record = {**_make_sqs_record({}, "msg-fmt"), "body": "BAD"}
        event      = {"Records": [bad_record]}
        response   = lambda_handler(event, None)

        assert "batchItemFailures" in response
        for entry in response["batchItemFailures"]:
            assert "itemIdentifier" in entry

    def test_full_batch_of_two_opportunities(self, mock_aws_resources):
        """Two valid records in one batch must each produce a DynamoDB entry."""
        opp2 = {**FINANCE_DESKTOP_MORNING_OPPORTUNITY}
        event = {
            "Records": [
                _make_sqs_record(SPORTS_MOBILE_EVENING_OPPORTUNITY, "msg-batch-1"),
                _make_sqs_record(opp2, "msg-batch-2"),
            ]
        }
        response = lambda_handler(event, None)
        assert response["batchItemFailures"] == []

        item1 = mock_aws_resources["table"].get_item(Key={"opportunity_id": "opp-001"}).get("Item")
        item2 = mock_aws_resources["table"].get_item(Key={"opportunity_id": "opp-002"}).get("Item")
        assert item1 is not None
        assert item2 is not None
