"""
AdFlow Latency Probe
====================

Sends a single ad-opportunity message through the full AdFlow pipeline and
breaks down where the time goes.

Segments measured
-----------------
  A) SQS send:          client → SQS input queue accepted
  B) SQS→Lambda delay:  SQS ingest → Lambda START (from CloudWatch)
  C) Lambda compute:    Lambda START → processed_at (from CloudWatch log line)
  D) DynamoDB write:    processed_at → Lambda END (from CloudWatch log line)
  E) Results delivery:  Lambda END → message visible in results queue + poll latency

  Total = A + B + C + D + E

Usage
-----
    python timing_probe.py --student-id gsalu [--region us-east-1] [--runs 5]

Requirements
------------
    boto3, AWS credentials in env / ~/.aws/credentials
    Student's Lambda must already be deployed (sam deploy done).

Notes
-----
    Lambda clock and local clock may differ by a second or two.
    "SQS→Lambda delay (B)" relies on CloudWatch INIT/START line timestamps,
    which are accurate to ~1 ms but require the log group to be accessible.
    If CloudWatch is unavailable, B+C+D are reported as a combined "Lambda phase."
"""

import argparse
import json
import time
import datetime
import re
import sys

import boto3
from botocore.exceptions import ClientError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ts_now_ms() -> float:
    """Wall-clock milliseconds (local machine)."""
    return time.perf_counter() * 1000  # relative, not epoch


def epoch_ms() -> float:
    """Epoch milliseconds (for timestamp comparisons with SQS/CW)."""
    return datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000


def iso_to_epoch_ms(iso: str) -> float:
    """Parse ISO-8601 string → epoch milliseconds."""
    dt = datetime.datetime.fromisoformat(iso.replace("Z", "+00:00"))
    return dt.timestamp() * 1000


def make_test_opportunity(run_id: int) -> dict:
    """Minimal valid opportunity that the Lambda can score."""
    return {
        "opportunity_id": f"probe-run{run_id:03d}-{int(epoch_ms())}",
        "timestamp": "2025-03-10T20:00:00Z",  # evening peak → time bonus applies
        "user_id": "u_probe",
        "content_category": "sports",
        "device_type": "mobile",
        "region": "northeast",
        "bids": [
            {"advertiser_id": "adv_sportswear_01", "bid_amount": 4.50, "category": "sportswear"},
            {"advertiser_id": "adv_energy_01",     "bid_amount": 5.00, "category": "energy_drink"},
            {"advertiser_id": "adv_fintech_01",    "bid_amount": 3.80, "category": "fintech"},
        ],
    }


# ---------------------------------------------------------------------------
# SQS helpers
# ---------------------------------------------------------------------------

def resolve_queue_url(sqs, name_or_url: str) -> str:
    if name_or_url.startswith("https://"):
        return name_or_url
    resp = sqs.get_queue_url(QueueName=name_or_url)
    return resp["QueueUrl"]


def poll_for_result(sqs, results_url: str, opportunity_id: str,
                    timeout_s: float = 30.0) -> tuple:
    """
    Poll the results queue until we see our message or timeout.

    Uses WaitTimeSeconds=0 + 100ms sleep between polls -- identical to what
    index.html does -- so the measured latency reflects what the browser sees.

    Returns (result_dict, t_received_ms, messages_skipped)
    where t_received_ms is epoch_ms() right after the matching receive call returns.
    """
    deadline = time.monotonic() + timeout_s
    skipped = 0

    while time.monotonic() < deadline:
        resp = sqs.receive_message(
            QueueUrl=results_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=0,   # non-blocking, matches browser behavior
            MessageAttributeNames=["All"],
        )
        t_received_ms = epoch_ms()

        for msg in resp.get("Messages", []):
            try:
                body = json.loads(msg["Body"])
            except (json.JSONDecodeError, KeyError):
                body = {}

            # Delete regardless of whether it's ours
            sqs.delete_message(
                QueueUrl=results_url,
                ReceiptHandle=msg["ReceiptHandle"],
            )

            if body.get("opportunity_id") == opportunity_id:
                return body, t_received_ms, skipped
            else:
                skipped += 1

        # 100ms between polls -- matches the browser's setTimeout(pollOnce, 100)
        time.sleep(0.100)

    return None, None, skipped


# ---------------------------------------------------------------------------
# CloudWatch helpers
# ---------------------------------------------------------------------------

def fetch_lambda_log_lines(logs_client, log_group: str, opportunity_id: str,
                            search_start_epoch_ms: int, timeout_s: float = 20.0) -> list:
    """
    Pull CloudWatch log events containing the opportunity_id from the Lambda log group.
    Returns a list of (timestamp_ms, message) tuples.
    """
    deadline = time.monotonic() + timeout_s
    found = []

    while time.monotonic() < deadline:
        try:
            resp = logs_client.filter_log_events(
                logGroupName=log_group,
                startTime=search_start_epoch_ms - 5000,  # 5 s buffer
                filterPattern=f'"{opportunity_id}"',
            )
            events = resp.get("events", [])
            if events:
                for e in events:
                    found.append((e["timestamp"], e["message"].rstrip()))
                return found
        except ClientError as e:
            if "ResourceNotFoundException" in str(e):
                return []
            raise

        time.sleep(2)

    return found


def parse_lambda_timing_from_logs(log_lines: list, opportunity_id: str) -> dict:
    """
    Extract timing info from Lambda CloudWatch log lines.

    Lambda emits lines like:
        START RequestId: xxxx Version: $LATEST
        [INFO] Processed probe-run001-xxx in 47.2 ms | winner=...
        END RequestId: xxxx
        REPORT RequestId: xxxx  Duration: 51.23 ms  Billed Duration: 52 ms ...

    Returns dict with keys: start_ts, end_ts, report_duration_ms, process_ms
    """
    result = {}

    for ts_ms, line in log_lines:
        # START line
        if line.startswith("START RequestId:"):
            result.setdefault("start_ts_ms", ts_ms)

        # END line
        if line.startswith("END RequestId:"):
            result["end_ts_ms"] = ts_ms

        # REPORT line — actual billed duration
        if line.startswith("REPORT RequestId:"):
            m = re.search(r"Duration:\s*([\d.]+)\s*ms", line)
            if m:
                result["report_duration_ms"] = float(m.group(1))
            m = re.search(r"Init Duration:\s*([\d.]+)\s*ms", line)
            if m:
                result["cold_start_ms"] = float(m.group(1))

        # Our own log line: "Processed <id> in X ms | ..."
        if opportunity_id in line and "in" in line and "ms" in line:
            m = re.search(r"in\s+([\d.]+)\s*ms", line)
            if m:
                result["lambda_process_ms"] = float(m.group(1))

    return result


# ---------------------------------------------------------------------------
# Single run
# ---------------------------------------------------------------------------

def run_once(sqs, logs_client, input_url: str, results_url: str,
             log_group: str, run_id: int, verbose: bool = True) -> dict:
    opp = make_test_opportunity(run_id)
    oid = opp["opportunity_id"]

    if verbose:
        print(f"\n{'─'*60}")
        print(f"  Run #{run_id} | opportunity_id = {oid}")
        print(f"{'─'*60}")

    # ── A: Send to input queue ───────────────────────────────────────────────
    t_before_send = epoch_ms()
    t0 = time.perf_counter()
    sqs.send_message(
        QueueUrl=input_url,
        MessageBody=json.dumps(opp),
    )
    t_after_send = epoch_ms()
    send_ms = (time.perf_counter() - t0) * 1000

    if verbose:
        print(f"  [A] SQS send completed in       {send_ms:7.1f} ms")
        print(f"      Polling results queue now...")

    # ── E: Poll for result (starts immediately after send) ──────────────────
    result, t_received_ms, skipped = poll_for_result(
        sqs, results_url, oid, timeout_s=30.0
    )

    if result is None:
        print(f"  ERROR: No result received within 30s for {oid}")
        return {}

    total_ms = t_received_ms - t_before_send
    processed_at_ms = iso_to_epoch_ms(result["processed_at"])

    # Time from send-complete to Lambda marking processed_at
    # (includes SQS→Lambda trigger delay + Lambda execution up to SQS result post)
    pipeline_ms = processed_at_ms - t_after_send

    # Time from Lambda processed_at to result appearing in queue + our poll returning
    result_delivery_ms = t_received_ms - processed_at_ms

    if verbose:
        print(f"  [Total] End-to-end:              {total_ms:7.1f} ms")
        print(f"  [A]     SQS send (client→queue): {send_ms:7.1f} ms")
        print(f"  [B+C+D] SQS trigger + Lambda:    {pipeline_ms:7.1f} ms  (processed_at clock may drift ±1s)")
        print(f"  [E]     Results delivery+poll:   {result_delivery_ms:7.1f} ms")
        if skipped:
            print(f"          (skipped {skipped} other messages in results queue)")

    # ── CloudWatch breakdown (B, C, D) ───────────────────────────────────────
    cw_timing = {}
    if logs_client and log_group:
        if verbose:
            print(f"\n  Fetching CloudWatch logs for Lambda internals...")
        log_lines = fetch_lambda_log_lines(
            logs_client, log_group, oid,
            search_start_epoch_ms=int(t_before_send),
            timeout_s=15.0,
        )
        if log_lines:
            cw_timing = parse_lambda_timing_from_logs(log_lines, oid)
            if verbose:
                if "start_ts_ms" in cw_timing and "end_ts_ms" in cw_timing:
                    lambda_wall_ms = cw_timing["end_ts_ms"] - cw_timing["start_ts_ms"]
                    trigger_delay_ms = cw_timing["start_ts_ms"] - t_after_send
                    print(f"\n  CloudWatch breakdown:")
                    print(f"  [B]   SQS→Lambda trigger delay:  {trigger_delay_ms:7.1f} ms")
                    print(f"  [C+D] Lambda wall time (CW):     {lambda_wall_ms:7.1f} ms")
                    if "lambda_process_ms" in cw_timing:
                        print(f"  [C]   Lambda compute+SQS send:   {cw_timing['lambda_process_ms']:7.1f} ms  (from Lambda log)")
                        dynamo_estimate = lambda_wall_ms - cw_timing["lambda_process_ms"]
                        print(f"  [D]   Estimated DynamoDB write:   {dynamo_estimate:7.1f} ms")
                    if "report_duration_ms" in cw_timing:
                        print(f"        REPORT Duration:            {cw_timing['report_duration_ms']:7.1f} ms")
                    if "cold_start_ms" in cw_timing:
                        print(f"  ⚠️   Cold start (Init):           {cw_timing['cold_start_ms']:7.1f} ms  ← adds to this run only")
                else:
                    print(f"  CloudWatch: found {len(log_lines)} lines but couldn't parse START/END")
                    for ts, line in log_lines:
                        print(f"    [{ts}] {line[:120]}")
        else:
            print(f"  CloudWatch: no log lines found yet (logs may be delayed)")

    return {
        "opportunity_id": oid,
        "total_ms": total_ms,
        "send_ms": send_ms,
        "pipeline_ms": pipeline_ms,
        "result_delivery_ms": result_delivery_ms,
        "cold_start": "cold_start_ms" in cw_timing,
        **cw_timing,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="AdFlow latency probe")
    parser.add_argument("--student-id", required=True, help="e.g. gsalu")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--runs", type=int, default=3,
                        help="Number of probe messages to send (default 3)")
    parser.add_argument("--no-cw", action="store_true",
                        help="Skip CloudWatch log fetching")
    args = parser.parse_args()

    sid = args.student_id
    input_q  = f"adflow-{sid}-input"
    results_q = f"adflow-{sid}-results"
    log_group = f"/aws/lambda/adflow-{sid}-worker"

    print(f"AdFlow Latency Probe")
    print(f"Student ID:    {sid}")
    print(f"Region:        {args.region}")
    print(f"Runs:          {args.runs}")
    print(f"Input queue:   {input_q}")
    print(f"Results queue: {results_q}")
    print(f"Log group:     {log_group}")

    sqs = boto3.client("sqs", region_name=args.region)
    logs_client = None if args.no_cw else boto3.client("logs", region_name=args.region)

    # Resolve full queue URLs once
    try:
        input_url = resolve_queue_url(sqs, input_q)
        results_url = resolve_queue_url(sqs, results_q)
        print(f"\nResolved input:   {input_url}")
        print(f"Resolved results: {results_url}")
    except ClientError as e:
        print(f"\nERROR resolving queue URLs: {e}")
        print("Make sure your AWS credentials are set and the Lambda is deployed.")
        sys.exit(1)

    results = []
    for i in range(1, args.runs + 1):
        r = run_once(sqs, logs_client, input_url, results_url, log_group,
                     run_id=i, verbose=True)
        if r:
            results.append(r)
        if i < args.runs:
            print(f"\n  Waiting 3s before next run...")
            time.sleep(3)

    # ── Summary table ─────────────────────────────────────────────────────────
    if len(results) > 1:
        print(f"\n{'═'*60}")
        print(f"  SUMMARY ({len(results)} runs)")
        print(f"{'═'*60}")

        def stats(key):
            vals = [r[key] for r in results if key in r]
            if not vals:
                return "  n/a"
            return f"  min={min(vals):6.0f}  avg={sum(vals)/len(vals):6.0f}  max={max(vals):6.0f}  ms"

        print(f"  Total end-to-end:      {stats('total_ms')}")
        print(f"  SQS send [A]:          {stats('send_ms')}")
        print(f"  SQS+Lambda [B+C+D]:    {stats('pipeline_ms')}")
        print(f"  Results delivery [E]:  {stats('result_delivery_ms')}")

        cw_available = any("start_ts_ms" in r for r in results)
        if cw_available:
            print(f"\n  CloudWatch breakdown:")
            # Compute trigger delays for runs that have CW data
            trigger_delays = []
            lambda_wall_times = []
            dynamo_estimates = []
            for r in results:
                if "start_ts_ms" in r and "send_ms" in r:
                    # Can't reconstruct t_after_send from stored data
                    pass
                if "report_duration_ms" in r:
                    lambda_wall_times.append(r["report_duration_ms"])
                if "lambda_process_ms" in r and "report_duration_ms" in r:
                    dynamo_estimates.append(r["report_duration_ms"] - r["lambda_process_ms"])

            if lambda_wall_times:
                avg = sum(lambda_wall_times)/len(lambda_wall_times)
                print(f"  Lambda REPORT Duration: avg={avg:.0f} ms  (all Lambda work)")
            if dynamo_estimates:
                avg = sum(dynamo_estimates)/len(dynamo_estimates)
                print(f"  Est. DynamoDB write:    avg={avg:.0f} ms  (= REPORT - logged process ms)")

        cold = [r for r in results if r.get("cold_start")]
        if cold:
            print(f"\n  ⚠️  Run(s) {[results.index(r)+1 for r in cold]} had cold starts — exclude from latency averages.")

    print(f"\nDone.")


if __name__ == "__main__":
    main()
