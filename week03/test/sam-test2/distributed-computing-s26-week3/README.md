# Week 3: Serverless API with Lambda, API Gateway, and DynamoDB

This project demonstrates a serverless REST API built with AWS Lambda, API Gateway, and DynamoDB.

## Architecture

- **API Gateway HTTP API**: Provides HTTP endpoints
- **AWS Lambda**: Handles business logic (GET and POST operations)
- **DynamoDB**: NoSQL database for storing items

## Project Structure

```
week03/
├── template.yaml          # SAM template defining infrastructure
├── local_tester.html      # Local HTML file for testing the API in your browser
├── src/
│   ├── handler.py        # Lambda function code
│   └── requirements.txt  # Python dependencies
├── events/               # Sample events for local testing
│   ├── post-event.json
│   └── get-event.json
└── README.md
```

## Prerequisites

1. AWS CLI installed and configured
2. AWS SAM CLI installed
3. Python 3.9 or later

### Installing SAM CLI

**macOS/Linux:**
```bash
brew install aws-sam-cli
```

**Windows:**
Download from: https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html

## Deployment

### 1. Build the application

```bash
sam build
```

### 2. Deploy to AWS

```bash
sam deploy --guided
```

Follow the prompts:
- Stack Name: `week3-serverless-api` (or your choice)
- AWS Region: `us-east-1` (or your choice)
- Confirm changes before deploy: `Y`
- Allow SAM CLI IAM role creation: `Y`
- Disable rollback: `N`
- Save arguments to configuration file: `Y`

### 3. Get the API URL

After deployment, SAM will output the API URL. Save this for testing.

## Testing

### Option 1: Local Web UI Tester (Easiest)

This repository includes a local HTML file for testing your deployed API directly from your browser.

1. Locate `local_tester.html` in the project directory
2. Open it in your web browser (right-click and select "Open with" your browser, or drag and drop into a browser window)
3. Paste your API URL from the deployment output into the API URL field
4. Click the buttons to test POST and GET requests
5. View real-time responses with status codes and JSON data

Note: This is a local testing tool that runs entirely in your browser. It uses the CORS configuration in the SAM template to make requests to your deployed API.

### Option 2: Command Line (curl)

**Create an item (POST):**

```bash
curl -X POST https://YOUR-API-URL/items \
  -H "Content-Type: application/json" \
  -d '{"id": "item-001", "name": "Laptop", "price": 999.99}'
```
or
Or 

```bash
curl -X POST https://your-api-url/items -H "Content-Type: application/json" -d "{\"id\": \"item-001\", \"name\": \"Laptop\", \"price\": 999.99}"
````

**Get an item (GET):**

```bash
curl https://YOUR-API-URL/items/item-001
```

## Local Testing

### Start the API locally

```bash
sam local start-api
```

### Test locally

```bash
# Create an item
curl -X POST http://localhost:3000/items \
  -H "Content-Type: application/json" \
  -d '{"id": "test-001", "name": "Test Item", "price": 19.99}'


# Get an item
curl http://localhost:3000/items/test-001
```

## Monitoring

### View Lambda logs

```bash
sam logs -n ApiFunction --stack-name week3-serverless-api --tail
```

### View CloudWatch logs in AWS Console

1. Go to CloudWatch Console
2. Navigate to Log Groups
3. Find `/aws/lambda/week3-serverless-api-ApiFunction-XXXXX`

## Cleanup

To delete all resources:

```bash
sam delete
```

## Understanding the Code

### Lambda Handler (src/handler.py)

The Lambda function handles two operations:
- **POST /items**: Creates a new item in DynamoDB
- **GET /items/{id}**: Retrieves an item by ID

Key concepts:
- Event structure from API Gateway
- DynamoDB operations (put_item, get_item)
- Error handling and HTTP status codes
- Decimal conversion for DynamoDB

### SAM Template (template.yaml)

Defines:
- DynamoDB table with partition key `id`
- Lambda function with environment variables
- API Gateway HTTP API with routes
- IAM permissions (automatically managed by SAM)

## Common Issues

### Issue: "Table does not exist"
- Make sure you deployed with `sam deploy`
- Check that TABLE_NAME environment variable is set

### Issue: "Access Denied"
- SAM automatically creates IAM roles
- Check that you allowed IAM role creation during deployment

### Issue: "Float types are not supported"
- DynamoDB requires Decimal type for numbers
- The handler automatically converts floats to Decimal

## Next Steps

Try extending this API:
- Add DELETE /items/{id} endpoint
- Add GET /items endpoint to list all items
- Add validation for required fields
- Add unit tests

