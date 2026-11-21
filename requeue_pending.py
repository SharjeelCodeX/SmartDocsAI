"""Requeue SmartDocs AI records that may have been left waiting by a stopped worker."""
import json

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from api import AWS_REGION, SQS_QUEUE_URL, documents_transaction

sqs = boto3.client("sqs", region_name=AWS_REGION)
PENDING_STATUSES = {"uploaded", "queued", "processing", "queue_failed"}

with documents_transaction() as documents:
    pending = [
        filename
        for filename, record in documents.items()
        if record.get("processing_status") in PENDING_STATUSES
    ]
    for filename in pending:
        documents[filename]["processing_status"] = "queued"
        documents[filename].pop("processing_error", None)

sent = 0
failed = []
for filename in pending:
    try:
        sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps({"filename": filename}),
        )
        sent += 1
    except (BotoCoreError, ClientError) as error:
        failed.append((filename, str(error)))
        with documents_transaction() as documents:
            if filename in documents:
                documents[filename]["processing_status"] = "queue_failed"
                documents[filename]["processing_error"] = str(error)

print(f"Pending records found: {len(pending)}")
print(f"Jobs sent to SQS: {sent}")
print(f"Jobs that failed to queue: {len(failed)}")
for filename, error in failed:
    print(f"FAILED: {filename}: {error}")