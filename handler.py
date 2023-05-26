import simplejson as json
import boto3
from decimal import Decimal

dynamoDb = boto3.resource('dynamodb', region_name='localhost', endpoint_url='http://localhost:8000')

from decimal import Decimal

def serialize(obj):
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: serialize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [serialize(item) for item in obj]
    return obj

def hello(event, context):
    table = dynamoDb.Table('bids')
    bids = None
    body = {
        "message": "Go Serverless v3.0! Your function executed successfully!",
        "input": event,
    }
    if 'bids' in event:
        bids = event['bids']
    else:
        print("no bids")
    initOptions = event.get("initOptions", None)

    if initOptions is not None:
        sortKey = initOptions.get("pubxId", None)

    with table.batch_writer(overwrite_by_pkeys=['bidId', 'sortKey']) as writer:
        for bid in bids:
            bid["sortKey"] = sortKey
            partition_key = bid.get("bidId", None)

            print(bid["bidId"],bid["sortKey"])
            if sortKey is not None and partition_key is not None:
                bid["sortKey"] = sortKey
                data = serialize(bid)
                writer.put_item(Item=data)

    print("Successfully wrote items")
    data = table.scan()
    print(data)
    return {"statusCode": 200, "body": "Hello from Lambda"}

def create_table(event, context):
    params = {
        'TableName': "bids",
        'KeySchema': [
            {'AttributeName': 'bidId', 'KeyType': 'HASH'},
            {'AttributeName': 'sortKey', 'KeyType': 'RANGE'},
        ],
        'AttributeDefinitions': [
            {'AttributeName': 'bidId', 'AttributeType': 'S'},
            {'AttributeName': 'sortKey', 'AttributeType': 'S'},
        ],
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    }
    table = dynamoDb.create_table(**params)
    print(f"Creating...")
    table.wait_until_exists()
    return table
