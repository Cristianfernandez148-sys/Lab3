import json
import boto3
import os
from PIL import Image
from io import BytesIO

s3 = boto3.client('s3')

def lambda_handler(event, context):
    for record in event['Records']:
        payload = json.loads(record['body'])
        
        bucket = payload['bucket']
        key = payload['key'] 

        filename = os.path.basename(key) 
        metadata_key = f"metadata/{filename}.json" 

        try:
            s3.head_object(Bucket=bucket, Key=metadata_key)
            print(f"Metadatos ya existen: {metadata_key}")
            continue
        except:
            pass

        print(f"Procesando: {key}")
        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            image_content = response['Body'].read()
            
            with Image.open(BytesIO(image_content)) as img:
                metadata = {
                    "source_bucket": bucket,  # <--- ¡AGREGA ESTA LÍNEA!
                    "original_key": key,
                    "format": img.format,
                    "width": img.width,
                    "height": img.height,
                    "mode": img.mode,
                    "size_bytes": len(image_content)
                }
                
            s3.put_object(
                Bucket=bucket,
                Key=metadata_key,
                Body=json.dumps(metadata),
                ContentType='application/json'
            )
            print(f"Guardado en: {metadata_key}")
            
        except Exception as e:
            print(f"Error procesando imagen {key}: {str(e)}")
            continue

    return {"statusCode": 200}