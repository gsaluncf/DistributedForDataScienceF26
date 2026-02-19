"""
AdFlow Test Apparatus - Local Web Server
=========================================

Serves the test apparatus HTML page and provides a REST API that proxies
requests to AWS SQS and DynamoDB using boto3. This avoids CORS issues
since the browser talks to localhost and the server talks to AWS.

The server uses the default boto3 credential chain (AWS CLI config,
environment variables, etc.) -- no credentials are entered in the browser.

Usage:
    python serve.py
    python serve.py --port 8000 --region us-east-1

Then open http://localhost:8000 in your browser.

API Endpoints:
    POST /api/generate      Generate opportunity messages (returns JSON)
    POST /api/send-batch    Send messages to an SQS queue
    POST /api/receive       Receive messages from an SQS queue
    POST /api/purge         Purge an SQS queue
    POST /api/cleanup-table Delete and recreate a DynamoDB table
    GET  /api/queue-stats   Get approximate message count for a queue
"""

import http.server
import json
import os
import sys
import time
import argparse
from urllib.parse import urlparse, parse_qs
from functools import partial

import boto3
from botocore.exceptions import ClientError

# Add project root to path so we can import the simulator
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from simulator.generator import generate_opportunities


class TestApparatusHandler(http.server.SimpleHTTPRequestHandler):
    """HTTP handler that serves static files and API endpoints."""

    def __init__(self, *args, region="us-east-1", **kwargs):
        self.region = region
        # SimpleHTTPRequestHandler.__init__ calls do_GET immediately,
        # so set directory before calling super().__init__
        super().__init__(*args, directory=os.path.dirname(os.path.abspath(__file__)), **kwargs)

    def _resolve_queue_url(self, queue_url_or_name, region):
        """Resolve a queue name to a full SQS URL if needed.

        Accepts either a full URL (https://sqs...) or a plain queue name
        (adflow-gsalu-input). If a plain name is provided, uses
        sqs.get_queue_url() to look up the full URL.
        """
        if not queue_url_or_name:
            return None
        if queue_url_or_name.startswith("https://"):
            return queue_url_or_name
        # Treat as a queue name and resolve
        sqs = boto3.client("sqs", region_name=region)
        resp = sqs.get_queue_url(QueueName=queue_url_or_name)
        return resp["QueueUrl"]

    def _send_json(self, status, data):
        """Send a JSON response with CORS headers."""
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self):
        """Read and parse JSON request body."""
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)
        return json.loads(raw) if raw else {}

    def do_OPTIONS(self):
        """Handle CORS preflight."""
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_POST(self):
        """Route POST requests to API handlers."""
        path = urlparse(self.path).path
        handlers = {
            "/api/generate": self._handle_generate,
            "/api/send-batch": self._handle_send_batch,
            "/api/receive": self._handle_receive,
            "/api/purge": self._handle_purge,
            "/api/cleanup-table": self._handle_cleanup_table,
        }
        handler = handlers.get(path)
        if handler:
            try:
                handler()
            except ClientError as e:
                self._send_json(500, {"error": str(e)})
            except Exception as e:
                self._send_json(500, {"error": str(e)})
        else:
            self._send_json(404, {"error": f"Unknown endpoint: {path}"})

    def do_GET(self):
        """Route GET requests -- API or static files."""
        path = urlparse(self.path).path
        if path == "/api/queue-stats":
            try:
                self._handle_queue_stats()
            except Exception as e:
                self._send_json(500, {"error": str(e)})
        else:
            super().do_GET()

    # -------------------------------------------------------------------
    # API handlers
    # -------------------------------------------------------------------

    def _handle_generate(self):
        """Generate opportunity messages and return them as JSON."""
        body = self._read_body()
        count = body.get("count", 10)
        seed = body.get("seed", 42)

        start = time.perf_counter()
        opportunities = generate_opportunities(count=count, seed=seed)
        elapsed = (time.perf_counter() - start) * 1000

        self._send_json(200, {
            "count": len(opportunities),
            "generation_ms": round(elapsed, 1),
            "opportunities": opportunities,
        })

    def _handle_send_batch(self):
        """Send messages to an SQS queue. Accepts up to 10 per call (SQS limit)."""
        body = self._read_body()
        queue_ref = body.get("queue_url")
        messages = body.get("messages", [])
        region = body.get("region", self.region)

        if not queue_ref:
            self._send_json(400, {"error": "queue_url is required"})
            return

        queue_url = self._resolve_queue_url(queue_ref, region)
        sqs = boto3.client("sqs", region_name=region)

        # SQS SendMessageBatch supports up to 10 messages
        entries = []
        for i, msg in enumerate(messages[:10]):
            entries.append({
                "Id": str(i),
                "MessageBody": json.dumps(msg) if isinstance(msg, dict) else msg,
            })

        if not entries:
            self._send_json(400, {"error": "No messages provided"})
            return

        start = time.perf_counter()
        response = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
        elapsed = (time.perf_counter() - start) * 1000

        successful = len(response.get("Successful", []))
        failed = len(response.get("Failed", []))

        self._send_json(200, {
            "sent": successful,
            "failed": failed,
            "send_ms": round(elapsed, 1),
        })

    def _handle_receive(self):
        """Receive and delete messages from an SQS queue."""
        body = self._read_body()
        queue_ref = body.get("queue_url")
        max_messages = min(body.get("max_messages", 10), 10)
        region = body.get("region", self.region)

        if not queue_ref:
            self._send_json(400, {"error": "queue_url is required"})
            return

        queue_url = self._resolve_queue_url(queue_ref, region)
        sqs = boto3.client("sqs", region_name=region)

        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=1,
        )

        messages = []
        for msg in response.get("Messages", []):
            try:
                parsed = json.loads(msg["Body"])
            except (json.JSONDecodeError, KeyError):
                parsed = msg.get("Body", "")

            messages.append(parsed)

            # Delete after reading
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=msg["ReceiptHandle"],
            )

        self._send_json(200, {
            "count": len(messages),
            "messages": messages,
        })

    def _handle_purge(self):
        """Purge all messages from an SQS queue."""
        body = self._read_body()
        queue_ref = body.get("queue_url")
        region = body.get("region", self.region)

        if not queue_ref:
            self._send_json(400, {"error": "queue_url is required"})
            return

        queue_url = self._resolve_queue_url(queue_ref, region)
        sqs = boto3.client("sqs", region_name=region)

        try:
            sqs.purge_queue(QueueUrl=queue_url)
            self._send_json(200, {"status": "purged", "queue_url": queue_url})
        except ClientError as e:
            if "PurgeQueueInProgress" in str(e):
                self._send_json(200, {
                    "status": "purge_already_in_progress",
                    "note": "SQS allows one purge per 60 seconds",
                })
            else:
                raise

    def _handle_cleanup_table(self):
        """Delete and recreate a DynamoDB table."""
        body = self._read_body()
        table_name = body.get("table_name")
        region = body.get("region", self.region)

        if not table_name:
            self._send_json(400, {"error": "table_name is required"})
            return

        dynamodb = boto3.client("dynamodb", region_name=region)

        # Delete the table
        try:
            dynamodb.delete_table(TableName=table_name)
            # Wait for deletion to complete
            waiter = dynamodb.get_waiter("table_not_exists")
            waiter.wait(TableName=table_name, WaiterConfig={"Delay": 2, "MaxAttempts": 30})
            print(f"Deleted table: {table_name}")
        except ClientError as e:
            if "ResourceNotFoundException" in str(e):
                print(f"Table {table_name} does not exist -- creating fresh")
            else:
                raise

        # Recreate with the standard schema
        dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[
                {"AttributeName": "opportunity_id", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "opportunity_id", "KeyType": "HASH"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        waiter = dynamodb.get_waiter("table_exists")
        waiter.wait(TableName=table_name, WaiterConfig={"Delay": 2, "MaxAttempts": 30})
        print(f"Recreated table: {table_name}")

        self._send_json(200, {"status": "recreated", "table_name": table_name})

    def _handle_queue_stats(self):
        """Get approximate message count for a queue."""
        params = parse_qs(urlparse(self.path).query)
        queue_ref = params.get("queue_url", [None])[0]
        region = params.get("region", [self.region])[0]

        if not queue_ref:
            self._send_json(400, {"error": "queue_url query parameter is required"})
            return

        queue_url = self._resolve_queue_url(queue_ref, region)
        sqs = boto3.client("sqs", region_name=region)

        attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
            ],
        )

        self._send_json(200, {
            "queue_url": queue_url,
            "approximate_messages": int(attrs["Attributes"].get("ApproximateNumberOfMessages", 0)),
            "in_flight": int(attrs["Attributes"].get("ApproximateNumberOfMessagesNotVisible", 0)),
        })

    def log_message(self, format, *args):
        """Override to add timestamp to log output."""
        sys.stderr.write(
            f"[{time.strftime('%H:%M:%S')}] {self.address_string()} - {format % args}\n"
        )


def main():
    parser = argparse.ArgumentParser(description="AdFlow Test Apparatus Server")
    parser.add_argument("--port", type=int, default=8000, help="Port (default: 8000)")
    parser.add_argument("--region", default="us-east-1", help="AWS region (default: us-east-1)")
    args = parser.parse_args()

    handler = partial(TestApparatusHandler, region=args.region)

    server = http.server.HTTPServer(("0.0.0.0", args.port), handler)
    print(f"AdFlow Test Apparatus running at http://localhost:{args.port}")
    print(f"AWS Region: {args.region}")
    print("Press Ctrl+C to stop.\n")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        server.shutdown()


if __name__ == "__main__":
    main()
