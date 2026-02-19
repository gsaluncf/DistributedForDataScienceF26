import json
import os
import boto3
from decimal import Decimal

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ.get('TABLE_NAME', 'items'))


# CORS headers to include in all responses
CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
}


def lambda_handler(event, context):
    """
    Main Lambda handler for API requests.
    Routes requests based on HTTP method.
    """
    # Get HTTP method from event (supports both v1.0 and v2.0 formats)
    http_method = event.get('httpMethod') or event.get('requestContext', {}).get('http', {}).get('method')

    if http_method == 'GET':
        return handle_get(event)
    elif http_method == 'POST':
        return handle_post(event)
    else:
        return {
            'statusCode': 405,
            'headers': CORS_HEADERS,
            'body': json.dumps({'error': 'Method not allowed'})
        }


def handle_get(event):
    """
    Handle GET requests to retrieve an item by ID.
    """
    # Extract item ID from path parameters
    item_id = event.get('pathParameters', {}).get('id')
    
    if not item_id:
        return {
            'statusCode': 400,
            'headers': CORS_HEADERS,
            'body': json.dumps({'error': 'Missing id parameter'})
        }

    try:
        # Get item from DynamoDB
        response = table.get_item(Key={'id': item_id})

        if 'Item' in response:
            # Convert Decimal to float for JSON serialization
            item = convert_decimals(response['Item'])
            return {
                'statusCode': 200,
                'headers': CORS_HEADERS,
                'body': json.dumps(item)
            }
        else:
            return {
                'statusCode': 404,
                'headers': CORS_HEADERS,
                'body': json.dumps({'error': 'Item not found'})
            }
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': CORS_HEADERS,
            'body': json.dumps({'error': str(e)})
        }


def handle_post(event):
    """
    Handle POST requests to create a new item.
    """
    try:
        # Parse request body
        body = json.loads(event.get('body', '{}'))
        
        # Validate required fields
        item_id = body.get('id')
        if not item_id:
            return {
                'statusCode': 400,
                'headers': CORS_HEADERS,
                'body': json.dumps({'error': 'Missing id in request body'})
            }

        # Convert floats to Decimal for DynamoDB
        item = convert_floats(body)

        # Store item in DynamoDB
        table.put_item(Item=item)

        return {
            'statusCode': 201,
            'headers': CORS_HEADERS,
            'body': json.dumps({'message': 'Item created', 'id': item_id})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': CORS_HEADERS,
            'body': json.dumps({'error': str(e)})
        }


def convert_floats(obj):
    """
    Recursively convert float values to Decimal for DynamoDB storage.
    """
    if isinstance(obj, list):
        return [convert_floats(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_floats(v) for k, v in obj.items()}
    elif isinstance(obj, float):
        return Decimal(str(obj))
    else:
        return obj


def convert_decimals(obj):
    """
    Recursively convert Decimal values to float for JSON serialization.
    """
    if isinstance(obj, list):
        return [convert_decimals(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        return float(obj)
    else:
        return obj

