# Week 4 Quiz (Cumulative: Weeks 3 & 4)

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

## Question 21: Latency vs Throughput
**How is throughput typically defined in a distributed system?**

A) The time it takes for a single request to complete
B) The number of requests a system can handle per unit of time
C) The amount of data stored in the database
D) The number of concurrent users

**Correct Answer: B**

---

## Question 22: Percentile Metrics
**Why are p99 latency metrics often more important than average response time?**

A) They show the experience of the fastest users
B) They reveal the experience of users suffering the worst performance
C) They are easier to calculate
D) They are the only metric AWS supports

**Correct Answer: B**

---

## Question 23: Amdahl's Law
**What is the main implication of Amdahl's Law for distributed systems?**

A) System speedup is limited by the portion of the task that cannot be parallelized
B) Adding more processors will always doublt the speed
C) Network latency is the only bottleneck
D) Data storage is the limiting factor

**Correct Answer: A**

---

## Question 24: Gustafson's Law
**How does Gustafson's Law differ from Amdahl's Law regarding scaling?**

A) It focuses on decreasing latency for fixed problem sizes
B) It suggests that as we add more processors, we can tackle larger problems efficiently
C) It proves that parallel computing is impossible
D) It only applies to quantum computing

**Correct Answer: B**

---

## Question 25: Scaling Models
**What is "Vertical Scaling" (scaling up)?**

A) Adding more servers to a cluster
B) Adding more data centers in different regions
C) Increasing the resources (CPU, RAM) of a single server
D) Splitting a database into shards

**Correct Answer: C**

---

## Question 26: Horizontal Scaling
**What is a primary advantage of Horizontal Scaling (scaling out) over Vertical Scaling?**

A) It is always easier to implement
B) Theoretically unlimited scaling by adding more commodity machines
C) No need for load balancers
D) Lower network latency

**Correct Answer: B**

---

## Question 27: Retries and Jitter
**What is the purpose of adding "Jitter" (randomness) to retry intervals?**

A) To make the system unpredictable
B) To prevent the "Thundering Herd" problem where all clients retry simultaneously
C) To increase the average latency
D) To confuse malicious attackers

**Correct Answer: B**

---

## Question 28: Idempotency
**What does it mean for an operation to be "idempotent"?**

A) It always fails on the second attempt
B) It can be applied multiple times without changing the result beyond the initial application
C) It requires authentication
D) It is only available in SQL databases

**Correct Answer: B**

---

## Question 29: Idempotency in Practice
**Which of the following HTTP methods is NOT typically idempotent?**

A) GET
B) PUT
C) DELETE
D) POST (when creating a new resource)

**Correct Answer: D**

---

## Question 30: SQS Visibility Timeout
**What happens when the "Visibility Timeout" expires for a message in SQS?**

A) The message is deleted from the queue
B) The message becomes visible again to other consumers and can be processed
C) The message is moved to a Dead Letter Queue
D) The queue is locked

**Correct Answer: B**

---

## Question 31: SQS vs SNS
**What is the key difference between SQS and SNS?**

A) SQS is "pull-based" (queues), while SNS is "push-based" (pub/sub).
B) SQS is faster than SNS.
C) SNS guarantees ordering, while SQS does not.
D) SNS is for file storage, SQS is for messaging.

**Correct Answer: A**

---

## Question 32: Dead Letter Queues (DLQ)
**What is the primary purpose of a Dead Letter Queue (DLQ)?**

A) To store messages that have expired
B) To archive all successful messages
C) To isolate messages that could not be processed successfully after max retries
D) To increase the throughput of the main queue

**Correct Answer: C**

---

## Question 33: Backpressure
**What is "Backpressure" in a distributed system?**

A) A mechanism for a consumer to signal a producer to slow down
B) The pressure on the database disk
C) A security feature to prevent DDoS attacks
D) The latency caused by network congestion

**Correct Answer: A**

---

## Question 34: Multi-AZ Deployment
**What is the main benefit of deploying across multiple Availability Zones (Multi-AZ)?**

A) Higher throughput
B) Lower cost
C) High Availability and fault tolerance against data center failures
D) Simpler deployment scripts

**Correct Answer: C**

---

## Question 35: Eventual Consistency
**In a distributed system, what does "Eventual Consistency" imply?**

A) Data will be consistent immediately after a write
B) Data may be stale for a short period, but will eventually become consistent across all nodes
C) Data is never guaranteed to be consistent
D) Only SQL databases support it

**Correct Answer: B**

---

## Question 36: SQS FIFO Queues
**When should you use an SQS FIFO queue instead of a Standard queue?**

A) When you need highest possible throughput
B) When the order of message processing is critical and must be preserved
C) When you don't care about duplicates
D) When you want cheaper pricing

**Correct Answer: B**

---

## Question 37: Fanout Pattern
**What is the "Fanout" pattern using SNS and SQS?**

A) Using many Lambda functions to process one message
B) Publishing a message to an SNS topic, which pushes it to multiple SQS queues for parallel processing
C) Sending messages from one SQS queue to multiple databases
D) Using a load balancer to distribute HTTP traffic

**Correct Answer: B**

---

## Question 38: Load Balancer vs Queue
**When handling a sudden spike in traffic, why might a Queue be better than just a Load Balancer + Auto Scaling Group?**

A) Queues act as a buffer/shock absorber, allowing consumers to process load at their own pace
B) Queues are faster than Load Balancers
C) Queues support WebSocket connections
D) Queues automatically scale the database

**Correct Answer: A**

---

## Question 39: CAP Theorem
**According to the CAP Theorem, which two properties can a distributed system satisfy simultaneously in the presence of a partition?**

A) Consistency and Availability
B) Consistency and Partition Tolerance
C) Availability and Partition Tolerance
D) Any two of the three

**Correct Answer: D**

---

## Question 40: Microservices Communication
**Which of the following creates tight coupling between services?**

A) Asynchronous messaging via SQS
B) Synchronous HTTP/REST calls
C) Pub/Sub via SNS
D) Event-driven architecture

**Correct Answer: B**
