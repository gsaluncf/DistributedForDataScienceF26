import boto3, json

iam = boto3.client('iam', region_name='us-east-1')

USER_NAME = 'sqs-lab-student'

# 1. Create user
try:
    iam.create_user(UserName=USER_NAME)
    print(f"Created IAM user: {USER_NAME}")
except iam.exceptions.EntityAlreadyExistsException:
    print(f"User '{USER_NAME}' already exists, skipping creation.")

# 2. Attach inline policy
policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "PizzaShopSQSOnly",
            "Effect": "Allow",
            "Action": [
                "sqs:SendMessage",
                "sqs:ReceiveMessage",
                "sqs:DeleteMessage",
                "sqs:GetQueueAttributes",
                "sqs:GetQueueUrl"
            ],
            "Resource": [
                "arn:aws:sqs:us-east-1:194722398367:pizza-orders",
                "arn:aws:sqs:us-east-1:194722398367:pizza-results"
            ]
        },
        {
            "Sid": "AllowIdentityCheck",
            "Effect": "Allow",
            "Action": "sts:GetCallerIdentity",
            "Resource": "*"
        }
    ]
}

iam.put_user_policy(
    UserName=USER_NAME,
    PolicyName='PizzaShopSQSOnly',
    PolicyDocument=json.dumps(policy)
)
print("Attached SQS-only policy.")

# 3. Generate access keys
keys = iam.create_access_key(UserName=USER_NAME)
creds = keys['AccessKey']

print("\n" + "=" * 50)
print("STUDENT CREDENTIALS (give these to students)")
print("=" * 50)
print(f"AWS_ACCESS_KEY_ID     = {creds['AccessKeyId']}")
print(f"AWS_SECRET_ACCESS_KEY = {creds['SecretAccessKey']}")
print("=" * 50)
print("\nThese creds can ONLY send/receive/delete messages")
print("on pizza-orders and pizza-results. Nothing else.")
