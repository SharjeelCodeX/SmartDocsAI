import json
import time
import threading
from datetime import datetime, timezone


import boto3
from botocore.exceptions import BotoCoreError, ClientError


from api import (
    AWS_REGION,
    SQS_QUEUE_URL,
    PROCESSING_MODE,
    PROCESSING_STALE_SECONDS,
    documents_transaction,
    load_documents,
    process_document,
)


MAX_ATTEMPTS = 5
POLL_WAIT_SECONDS = 20
VISIBILITY_TIMEOUT_SECONDS = 300
HEARTBEAT_INTERVAL_SECONDS = 120
RECONCILE_INTERVAL_SECONDS = 60
REQUEUE_COOLDOWN_SECONDS = 300



sqs = boto3.client("sqs", region_name=AWS_REGION)

if PROCESSING_MODE != "sqs":
    raise RuntimeError(
        "SQS worker will not start unless PROCESSING_MODE=sqs. "
        "Set local API containers to PROCESSING_MODE=local."
    )

print("SmartDocs AI worker started. Waiting for jobs...", flush=True)


def extend_visibility(receipt):
    """Keep a long-running document job invisible while it is processing."""
    try:
        sqs.change_message_visibility(
            QueueUrl=SQS_QUEUE_URL,
            ReceiptHandle=receipt,
            VisibilityTimeout=VISIBILITY_TIMEOUT_SECONDS,
        )
    except (BotoCoreError, ClientError) as error:
        print(f"Visibility extension failed: {error}", flush=True)



def heartbeat(receipt, stop_event):
    """Periodically extend visibility while a document is processing."""
    while not stop_event.wait(HEARTBEAT_INTERVAL_SECONDS):
        extend_visibility(receipt)



def acknowledge_message(receipt):
    """Delete a message only after success or confirmed completion."""
    sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt)



def _parse_timestamp(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None



def reconcile_jobs():
    """Requeue recoverable metadata records after queue-send or worker crashes."""
    now = datetime.now(timezone.utc)
    candidates = []
    with documents_transaction() as documents:
        for filename, record in documents.items():
            status = record.get("processing_status")
            if status in {"completed", "needs_ocr", "failed"}:
                continue
            if status == "processing":
                started = _parse_timestamp(record.get("processing_started_at"))
                if started and (now - started).total_seconds() < PROCESSING_STALE_SECONDS:
                    continue
                record["processing_status"] = "queued"
                record.pop("processing_started_at", None)
                record["processing_error"] = "Recovered stale processing job; requeued."
            elif status not in {"queued", "queue_failed"}:
                continue
            last_sent = _parse_timestamp(record.get("queue_last_enqueued_at"))
            if last_sent and (now - last_sent).total_seconds() < REQUEUE_COOLDOWN_SECONDS:
                continue
            candidates.append(filename)

    for filename in candidates:
        try:
            sqs.send_message(
                QueueUrl=SQS_QUEUE_URL,
                MessageBody=json.dumps({"filename": filename}),
            )
            with documents_transaction() as documents:
                if filename in documents and documents[filename].get("processing_status") not in {"completed", "needs_ocr"}:
                    documents[filename]["processing_status"] = "queued"
                    documents[filename]["queue_last_enqueued_at"] = now.isoformat()
                    documents[filename].pop("processing_error", None)
            print(f"Requeued recoverable job: {filename}", flush=True)
        except (BotoCoreError, ClientError) as error:
            with documents_transaction() as documents:
                if filename in documents:
                    documents[filename]["processing_status"] = "queue_failed"
                    documents[filename]["processing_error"] = str(error)
            print(f"Requeue failed for {filename}: {error}", flush=True)



last_reconcile = 0
while True:
    try:
        if time.monotonic() - last_reconcile >= RECONCILE_INTERVAL_SECONDS:
            reconcile_jobs()
            last_reconcile = time.monotonic()

        response = sqs.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=POLL_WAIT_SECONDS,
            VisibilityTimeout=VISIBILITY_TIMEOUT_SECONDS,
            AttributeNames=["ApproximateReceiveCount"],
        )

        for message in response.get("Messages", []):
            receipt = message["ReceiptHandle"]
            attempts = int(message.get("Attributes", {}).get("ApproximateReceiveCount", "1"))
            filename = None

            try:
                job = json.loads(message["Body"])
                filename = job.get("filename")
                if (
                    not isinstance(filename, str)
                    or not filename
                    or "/" in filename
                    or "\\" in filename
                ):
                    raise ValueError("Queue message contains an invalid filename.")



                record = load_documents().get(filename)
                if record is None:
                    raise RuntimeError(
                        f"Metadata record missing for {filename}. Message will be retried and eventually moved to the DLQ."
                    )
                status = record.get("processing_status")
                if status in {"completed", "needs_ocr"}:
                    acknowledge_message(receipt)
                    print(f"Skipped already finished document: {filename} ({status})", flush=True)
                    continue



                print(f"Processing: {filename} (attempt {attempts}/{MAX_ATTEMPTS})", flush=True)
                stop_event = threading.Event()
                heartbeat_thread = threading.Thread(
                    target=heartbeat, args=(receipt, stop_event), daemon=True
                )
                heartbeat_thread.start()
                try:
                    result_status = process_document(filename)
                finally:
                    stop_event.set()
                    heartbeat_thread.join(timeout=5)

                if result_status not in {"completed", "needs_ocr"}:
                    raise RuntimeError(f"Processing did not finish successfully: {result_status}")
                acknowledge_message(receipt)
                print(f"Finished: {filename} ({result_status})", flush=True)



            except Exception as error:
                print(f"Job failed (attempt {attempts}/{MAX_ATTEMPTS}): {error}", flush=True)
                if attempts >= MAX_ATTEMPTS:
                    try:
                        if filename:
                            with documents_transaction() as documents:
                                if filename in documents:
                                    documents[filename]["processing_status"] = "failed"
                                    documents[filename].pop("processing_started_at", None)
                                    documents[filename]["processing_error"] = (
                                        f"Stopped after {MAX_ATTEMPTS} attempts. Last error: {error}"
                                    )
                        print(
                            f"Maximum attempts reached for {filename}. Leaving message for SQS dead-letter handling.",
                            flush=True,
                        )
                    except Exception as final_error:
                        print(f"Could not finalize failed job: {final_error}", flush=True)
                        # Do not delete if final metadata update fails; SQS will retry.



    except (BotoCoreError, ClientError) as error:
        print(f"SQS connection error: {error}", flush=True)
        time.sleep(5)
    except Exception as error:
        print(f"Unexpected worker error: {error}", flush=True)
        time.sleep(5)
