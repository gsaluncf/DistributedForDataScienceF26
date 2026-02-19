# Week 3 — Quiz Seeds (20 Multiple-Choice Questions)

## Question 1: Serverless Architecture
**What is the primary benefit of serverless architecture compared to traditional server-based architecture?**

A) Serverless is always cheaper than traditional servers
B) You don't need to manage or provision servers
C) Serverless applications run faster than traditional applications
D) Serverless eliminates the need for code deployment

**Correct Answer: B**

---

## Question 2: AWS SAM
**What does AWS SAM (Serverless Application Model) provide?**

A) A graphical user interface for building serverless applications
B) A simplified syntax for defining serverless resources as Infrastructure as Code
C) A database specifically designed for serverless applications
D) A monitoring tool for Lambda functions

**Correct Answer: B**

---

## Question 3: Lambda Handler
**In a Python Lambda function, what are the two required parameters for the handler function?**

A) request and response
B) event and context
C) input and output
D) data and metadata

**Correct Answer: B**

---

## Question 4: Lambda Cold Starts
**What causes a Lambda cold start?**

A) The function hasn't been invoked recently and AWS needs to initialize a new container
B) The function code has syntax errors
C) The DynamoDB table is not responding
D) The API Gateway is experiencing high traffic

**Correct Answer: A**

---

## Question 5: Lambda Optimization
**Why should you initialize resources like boto3 clients OUTSIDE the lambda_handler function?**

A) It's required by AWS Lambda
B) It prevents syntax errors
C) Initialized resources are reused across warm starts, improving performance
D) It reduces the size of the deployment package

**Correct Answer: C**

---

## Question 6: API Gateway Event Format
**In API Gateway HTTP API v2.0 format, how do you access the HTTP method in the Lambda event object?**

A) event['method']
B) event['httpMethod']
C) event['requestContext']['http']['method']
D) event['request']['method']

**Correct Answer: C**

---

## Question 7: Lambda Response Format
**What two fields are REQUIRED in a Lambda function's response for API Gateway integration?**

A) statusCode and body
B) status and data
C) code and message
D) response and headers

**Correct Answer: A**

---

## Question 8: DynamoDB Partition Key
**What is the purpose of a partition key in DynamoDB?**

A) To sort items within a table
B) To determine which physical partition stores the item
C) To encrypt data at rest
D) To create relationships between tables

**Correct Answer: B**

---

## Question 9: DynamoDB Data Types
**Why must you convert Python float values to Decimal before storing them in DynamoDB?**

A) DynamoDB doesn't support the float data type
B) Floats are too large for DynamoDB storage
C) It's required for encryption
D) It improves query performance

**Correct Answer: A**

---

## Question 10: DynamoDB Billing Mode
**What is the difference between PAY_PER_REQUEST and PROVISIONED billing modes in DynamoDB?**

A) PAY_PER_REQUEST charges per request; PROVISIONED requires pre-allocated capacity
B) PAY_PER_REQUEST is only for development; PROVISIONED is for production
C) PAY_PER_REQUEST is slower than PROVISIONED
D) There is no difference; they are aliases for the same mode

**Correct Answer: A**

---

## Question 11: Hot Partitions
**What causes a "hot partition" problem in DynamoDB?**

A) Too many requests to items with the same partition key
B) The table is too large
C) The Lambda function is making too many requests
D) The API Gateway is misconfigured

**Correct Answer: A**

---

## Question 12: DynamoDB Operations
**Which boto3 method is used to retrieve a single item from DynamoDB by its primary key?**

A) query()
B) scan()
C) get_item()
D) fetch_item()

**Correct Answer: C**

---

## Question 13: REST API Methods
**Which HTTP method should be used to CREATE a new resource in a REST API?**

A) GET
B) POST
C) PUT
D) DELETE

**Correct Answer: B**

---

## Question 14: HTTP Status Codes
**What HTTP status code should be returned when a requested resource is not found?**

A) 400 Bad Request
B) 401 Unauthorized
C) 404 Not Found
D) 500 Internal Server Error

**Correct Answer: C**

---

## Question 15: SAM Build Command
**What does the `sam build` command do?**

A) Deploys your application to AWS
B) Downloads dependencies and packages your code for deployment
C) Deletes existing resources
D) Tests your Lambda function locally

**Correct Answer: B**


---

## Question 16: SAM Deploy
**What does the `--guided` flag do in the `sam deploy --guided` command?**

A) Automatically fixes errors in your template
B) Walks you through configuration prompts interactively
C) Deploys to multiple regions simultaneously
D) Creates a backup of your existing stack

**Correct Answer: B**

---

## Question 17: IAM Permissions
**In the SAM template, what does the `DynamoDBCrudPolicy` provide to the Lambda function?**

A) Permission to create new DynamoDB tables
B) Permission to read, write, update, and delete items in the specified table
C) Permission to delete the DynamoDB table
D) Permission to view CloudWatch logs

**Correct Answer: B**

---

## Question 18: CloudWatch Logs
**Where can you find the execution logs for a Lambda function?**

A) In the Lambda function's source code directory
B) In CloudWatch Logs under /aws/lambda/[function-name]
C) In the API Gateway console
D) In the DynamoDB table

**Correct Answer: B**

---

## Question 19: Error Handling
**What should a Lambda function return when it encounters an error that should result in a 400 Bad Request response?**

A) Raise a Python exception
B) Return None
C) Return a dictionary with statusCode: 400 and an error message in the body
D) Call sys.exit(400)

**Correct Answer: C**

---

## Question 20: SAM Cleanup
**What command should you use to delete all resources created by a SAM deployment?**

A) sam destroy
B) sam delete --stack-name [stack-name]
C) sam remove
D) aws cloudformation delete-stack

**Correct Answer: B**

---

## Notes for Instructor

- These 20 questions cover all major topics from Week 3: serverless concepts, Lambda, API Gateway, DynamoDB, SAM, REST APIs, error handling, and best practices.
- Questions are designed to test both conceptual understanding and practical implementation knowledge.
- Correct answers are clearly marked for easy quiz creation in Canvas.
- Consider randomizing answer order when importing to Canvas to prevent pattern recognition.
