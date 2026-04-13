# AdFlow — Real-Time Ad Selection Pipeline
**Project 1 · Distributed Systems for Data Science · New College of Florida**

## Student
Matthew Macfarlane (`macfarlane`)

---

## Quick Start

### 1. Create and activate a virtual environment
```bash
python -m venv .venv
# macOS / Linux
source .venv/bin/activate
# Windows PowerShell
.venv\Scripts\Activate.ps1
```

### 2. Install test dependencies
```bash
pip install moto[sqs,dynamodb] pytest
```

### 3. Run tests (all 23 must pass)
```bash
pytest worker/tests/test_handler.py -v
```

### 4. Deploy
```bash
sam build
sam deploy --guided --stack-name adflow-macfarlane
# Parameter StudentId: macfarlane
```

### 5. Verify deployment
```bash
aws lambda get-function --function-name adflow-macfarlane-worker
aws sqs get-queue-url --queue-name adflow-macfarlane-input
aws dynamodb describe-table --table-name adflow-macfarlane-results
```

### 6. Stream logs
```bash
aws logs tail /aws/lambda/adflow-macfarlane-worker --follow
```

### 7. Cleanup between runs
```bash
python cleanup.py --student-id macfarlane
```

### 8. Tear down when done
```bash
sam delete --stack-name adflow-macfarlane
```

---

## Project Structure
```
.
├── template.yaml              # SAM template — all AWS resources
├── iam-policy.json            # Scoped IAM policy for adflow-tester user
├── cleanup.py                 # Purge queues + recreate DynamoDB table
├── worker/
│   ├── lambda_handler.py      # Bid engine — Tasks 1–4
│   ├── requirements.txt       # Intentionally empty (boto3 in Lambda runtime)
│   └── tests/
│       └── test_handler.py    # 23 tests across 3 suites
├── analysis/
│   └── analysis.ipynb         # Analyst report — pipeline evidence + Q1 + Q2
└── screenshots/
    └── burst_run.png          # Test apparatus screenshot (add after Burst run)
```

---

## Scoring Formula
```
score = bid_amount × relevance_multiplier × time_bonus × device_bonus
```

| Content | Advertiser | Multiplier |
|---------|-----------|-----------|
| sports | sportswear | 1.4 |
| sports | energy_drink | 1.3 |
| finance | fintech | 1.5 |
| finance | insurance | 1.3 |
| entertainment | streaming | 1.4 |
| entertainment | gaming | 1.3 |
| lifestyle | beauty | 1.3 |
| lifestyle | travel | 1.2 |
| any | any other | 1.0 |

| UTC Hours | Bonus |
|-----------|-------|
| 06–08 | 1.20 |
| 12–13 | 1.15 |
| 19–22 | 1.25 |
| Other | 1.00 |

| Device | Bonus |
|--------|-------|
| mobile | 1.1 |
| desktop | 1.0 |
