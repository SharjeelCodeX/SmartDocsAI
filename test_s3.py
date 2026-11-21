
import boto3

BUCKET_NAME = "smartdocs-ai-documents-2026-unique123"

s3 = boto3.client("s3", region_name="eu-north-1")
response = s3.list_objects_v2(Bucket=BUCKET_NAME)

print("Connected to AWS S3 successfully!")
print("Bucket:", BUCKET_NAME)
print("Objects found:", response.get("KeyCount", 0))
