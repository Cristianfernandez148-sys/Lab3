import json
import os
import urllib.parse

import boto3

sqs = boto3.client("sqs")

ALLOWED_EXTS = (".jpg", ".jpeg", ".png")

def lambda_handler(event, context):
    queue_url = os.environ["QUEUE_URL"]

    # S3 event can have multiple records
    for record in event.get("Records", []):
        s3_info = record.get("s3", {})
        bucket = s3_info.get("bucket", {}).get("name")
        key = s3_info.get("object", {}).get("key")
        etag = s3_info.get("object", {}).get("eTag") or s3_info.get("object", {}).get("etag")

        if not bucket or not key:
            continue

        key = urllib.parse.unquote_plus(key)

        # Loop prevention: only incoming/ triggers processing  [oai_citation:13‡Lab_3.pdf](sediment://file_0000000082a471f584a6051fe1c4bbdf)
        if not key.startswith("incoming/"):
            continue

        # Validate file type  [oai_citation:14‡Lab_3.pdf](sediment://file_0000000082a471f584a6051fe1c4bbdf)
        lower = key.lower()
        if not lower.endswith(ALLOWED_EXTS):
            continue

        msg = {"bucket": bucket, "key": key, "etag": etag}
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(msg),
        )

    return {"ok": True}