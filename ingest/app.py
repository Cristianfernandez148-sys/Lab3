import json
import os
import boto3

sqs = boto3.client('sqs')
QUEUE_URL = os.environ['QUEUE_URL']

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        etag = record['s3']['object']['eTag']

        file_extension = os.path.splitext(key)[1].lower()
        if file_extension not in ['.jpg', '.jpeg', '.png']:
            print(f"Archivo ignorado (no es imagen): {key}")
            continue

        message_body = {
            'bucket': bucket,
            'key': key,
            'etag': etag
        }

        response = sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(message_body)
        )
        print(f"Enviado a SQS: {key} - MessageId: {response['MessageId']}")
    
    return {"statusCode": 200}