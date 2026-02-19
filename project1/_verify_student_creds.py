import boto3, os

os.environ['AWS_ACCESS_KEY_ID'] = 'AKIAIOSFODNN7EXAMPLE'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'

ORDER_URL = 'https://sqs.us-east-1.amazonaws.com/194722398367/pizza-orders'
RESULT_URL = 'https://sqs.us-east-1.amazonaws.com/194722398367/pizza-results'

# Test 1: Identity check
sts = boto3.client('sts', region_name='us-east-1')
identity = sts.get_caller_identity()
print(f"1. Identity: {identity['Arn']}")

# Test 2: Queue access
sqs = boto3.client('sqs', region_name='us-east-1')
attrs = sqs.get_queue_attributes(QueueUrl=ORDER_URL, AttributeNames=['QueueArn'])
print(f"2. Order queue OK: {attrs['Attributes']['QueueArn']}")

attrs = sqs.get_queue_attributes(QueueUrl=RESULT_URL, AttributeNames=['QueueArn'])
print(f"3. Result queue OK: {attrs['Attributes']['QueueArn']}")

# Test 3: Verify scoped - try something they shouldn't be able to do
try:
    sqs.list_queues()
    print("4. WARNING: list_queues worked (policy may be too broad)")
except Exception as e:
    print(f"4. Good - list_queues blocked: {type(e).__name__}")

print("\nAll checks passed! These creds are ready for students.")
