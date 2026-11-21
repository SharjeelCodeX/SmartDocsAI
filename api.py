from pathlib import Path

from datetime import datetime, timezone

import copy
import os
import tempfile
from contextlib import contextmanager

import json

import re

import uuid

import pytesseract

from PIL import Image

from pypdf import PdfReader

from pdf2image import convert_from_path

from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks

from fastapi.responses import FileResponse

import boto3

from botocore.exceptions import BotoCoreError, ClientError

import shutil


BASE_DIR = Path(__file__).resolve().parent

UPLOAD_FOLDER = BASE_DIR / "uploads"
METADATA_FILE = BASE_DIR / "documents.json"
STATIC_FOLDER = BASE_DIR / "static"

app = FastAPI(title="SmartDocs AI")

@app.get("/app", include_in_schema=False)
def application():
    return FileResponse(STATIC_FOLDER / "index.html")

MAX_FILE_SIZE = 10 * 1024 * 1024


S3_BUCKET = os.getenv(
    "S3_BUCKET",
    "smartdocs-ai-documents-2026-unique123"
)

AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")

# local: API processes uploads in background; sqs: production worker processes jobs.
PROCESSING_MODE = os.getenv("PROCESSING_MODE", "local").strip().lower()
PROCESSING_STALE_SECONDS = int(os.getenv("PROCESSING_STALE_SECONDS", "900"))

SQS_QUEUE_URL = os.getenv(
    "SQS_QUEUE_URL",
    "https://sqs.eu-north-1.amazonaws.com/556957333669/smartdocs-ai-processing"
)


s3_client = boto3.client(

    "s3",

    region_name=AWS_REGION

)

sqs_client = boto3.client(

    "sqs",

    region_name=AWS_REGION

)

POPPLER_PATH = os.getenv(
    "POPPLER_PATH",
    r"C:\Users\hp\Downloads\Release-26.09.0-0\poppler-26.09.0\Library\bin"
)

TESSERACT_PATH = os.getenv(
    "TESSERACT_PATH",
    r"C:\Program Files\Tesseract-OCR\tesseract.exe"
)

if shutil.which("tesseract"):
    pytesseract.pytesseract.tesseract_cmd = shutil.which("tesseract")
else:
    pytesseract.pytesseract.tesseract_cmd = TESSERACT_PATH

if not os.path.isdir(POPPLER_PATH):
    POPPLER_PATH = None

# -----------------------------

# 1. DOCUMENT METADATA

# -----------------------------

def _read_documents_unlocked():
    """Read metadata while the caller holds the cross-process file lock."""
    if not METADATA_FILE.exists():
        return {}
    try:
        data = json.loads(METADATA_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        backup = METADATA_FILE.with_name("documents_invalid_backup.json")
        try:
            if not backup.exists():
                backup.write_bytes(METADATA_FILE.read_bytes())
        except OSError:
            pass
        raise RuntimeError(
            f"Metadata file is invalid JSON ({error}). A backup was attempted at {backup}."
        ) from error
    if not isinstance(data, dict):
        raise RuntimeError("Metadata file must contain a JSON object keyed by saved filename.")
    return data

@contextmanager
def _metadata_lock():
    """Cross-process lock for Windows and POSIX, shared by API and worker."""
    lock_path = METADATA_FILE.with_name(METADATA_FILE.name + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "a+b") as lock_file:
        lock_file.seek(0, os.SEEK_END)
        if lock_file.tell() == 0:
            lock_file.write(b"0")
            lock_file.flush()
        lock_file.seek(0)
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                lock_file.seek(0)
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

def _write_documents_unlocked(documents):
    """Atomically replace the JSON file; caller must hold _metadata_lock."""
    METADATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(documents, indent=4, ensure_ascii=False)
    fd, temp_name = tempfile.mkstemp(
        prefix=METADATA_FILE.name + ".", suffix=".tmp", dir=str(METADATA_FILE.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as temp_file:
            temp_file.write(payload)
            temp_file.flush()
            os.fsync(temp_file.fileno())
        os.replace(temp_name, METADATA_FILE)
    finally:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass

def load_documents():
    with _metadata_lock():
        return _read_documents_unlocked()

def save_documents(documents):
    """Compatibility helper. Prefer documents_transaction for read/modify/write."""
    with _metadata_lock():
        _write_documents_unlocked(documents)

@contextmanager
def documents_transaction():
    """Lock, load, mutate, and atomically save metadata as one operation."""
    with _metadata_lock():
        documents = _read_documents_unlocked()
        original = copy.deepcopy(documents)
        yield documents
        if documents != original:
            _write_documents_unlocked(documents)

# 2. TEXT MATCHING HELPERS

# -----------------------------

def find_value(patterns, text):

    for pattern in patterns:

        match = re.search(

            pattern,

            text,

            re.IGNORECASE | re.MULTILINE

        )

        if match:

            for value in match.groups():

                if value is not None and value.strip():

                    return value.strip()

    return None

def labeled_value(labels, text):

    return find_value([

        rf"^[ \t]*[.,;:\_-]?[ \t]*(?:{labels})[ \t]*(?::[ \t]*([^\r\n]+)|[ \t]*\r?\n[ \t]*([^\r\n]+))"

    ], text)

def parse_amount(value):

    """Convert a monetary string into a number for validation."""

    if not value:

        return None

    cleaned = re.sub(r"[^\d,.\-]", "", value)

    cleaned = cleaned.replace(",", "")

    try:

        return float(cleaned)

    except ValueError:

        return None

def find_invoice_amount(text, label_patterns):

    """

    Find an amount on a specifically labelled invoice line.

    Skips percentages such as 8.00% and returns the final monetary

    amount on a matching line, which supports tax lines containing

    both a percentage and an actual tax amount.

    """

    amount_pattern = re.compile(

        r"(?<![\w.])(?:[$€£₹]\s*)?[+-]?\s*"

        r"\d[\d,]*\.\d{2}(?!\s*%)"

    )

    lines = text.splitlines()

    # Check labels in priority order.

    for label_pattern in label_patterns:

        for line in lines:

            if not re.search(

                label_pattern,

                line,

                re.IGNORECASE

            ):

                continue

            matches = list(amount_pattern.finditer(line))

            if matches:

                return matches[-1].group(0).strip()

    return None

# -----------------------------

# 3. DOCUMENT CLASSIFICATION

# -----------------------------

def classify_document(text):

    content = text.lower()

    scores = {

        "invoice": 0,

        "receipt": 0,

        "resume": 0,

        "contract": 0,

        "bank_statement": 0,

        "maintenance_notice": 0,

        "assignment": 0

    }

    for keyword in [

        "invoice number", "invoice no", "invoice date",

        "bill to", "amount due", "subtotal",

        "tax amount", "payment terms"

    ]:

        if keyword in content:

            scores["invoice"] += 2

    if re.search(r"\binvoice\b", content):

        scores["invoice"] += 3

    for keyword in [

        "receipt", "change due", "cashier",

        "payment received", "transaction id",

        "paid by card", "till "

    ]:

        if keyword in content:

            scores["receipt"] += 2

    for keyword in [

        "work experience", "professional experience",

        "technical skills", "employment history",

        "career objective", "work history"

    ]:

        if keyword in content:

            scores["resume"] += 2

    for keyword in [

        "terms and conditions", "whereas",

        "party of the first part", "agreement",

        "governing law", "effective date",

        "confidentiality", "licensor", "licensee",

        "lessor", "lessee", "rental agreement"

    ]:

        if keyword in content:

            scores["contract"] += 2

    strong_contract = (

        "rental agreement" in content

        or ("licensor:" in content and "licensee:" in content)

        or ("landlord:" in content and "tenant:" in content)

    )

    for keyword in [

        "bank statement", "account summary",

        "statement period", "opening balance",

        "closing balance", "transaction history",

        "account number", "deposits and withdrawals"

    ]:

        if keyword in content:

            scores["bank_statement"] += 2

    for keyword in [

        "maintenance notice",

        "notice of planned maintenance",

        "planned maintenance",

        "scheduled maintenance",

        "building maintenance",

        "maintenance work",

        "service interruption",

        "maintenance schedule"

    ]:

        if keyword in content:

            scores["maintenance_notice"] += 2

    for keyword in [

        "assignment", "roll no", "roll number",

        "submitted to", "course title",

        "student name", "total marks"

    ]:

        if keyword in content:

            scores["assignment"] += 2

    if strong_contract:

        document_type = "contract"

    else:

        highest_score = max(scores.values())

        winners = [

            name for name, score in scores.items()

            if score == highest_score

        ]

        if highest_score < 2 or len(winners) != 1:

            document_type = "unknown"

        else:

            document_type = winners[0]

    return {

        "document_type": document_type,

        "classification_scores": scores

    }

# -----------------------------

# 4. STRUCTURED FIELD EXTRACTION

# -----------------------------

def find_supplier_name(text):

    supplier = labeled_value(

        r"supplier|vendor|seller|issued\s+by|from|company\s+name",

        text

    )

    if supplier:

        return supplier

    for line in text.splitlines()[:8]:

        line = line.strip()

        if not line or len(line) < 3:

            continue

        if re.search(

            r"invoice|bill\s+to|billed\s+to|customer|invoice\s+date|"

            r"^\d|@|https?://",

            line,

            re.IGNORECASE

        ):

            continue

        if re.search(

            r"\b(inc\.?|llc|ltd\.?|limited|corp\.?|corporation|"

            r"logistics|technologies|industries)\b",

            line,

            re.IGNORECASE

        ):

            return line

    return None

def extract_structured_data(text, extraction_method="pdf_text"):

    classification = classify_document(text)

    document_type = classification["document_type"]

    fields = {}

    required_fields = []

    validation_warnings = []

    is_ocr = extraction_method in {

        "tesseract_ocr",

        "tesseract_ocr_pdf"

    }

    if document_type == "invoice":

        # Extract a supplier only when a recognized label identifies one.

        fields = {

            "invoice_number": find_value([

                r"^[ \t]*invoice[ \t]*(?:number|no\.?|#)"

                r"[ \t]*:?[ \t]*\r?\n?[ \t]*"

                r"([A-Z0-9][A-Z0-9/-]*)"

            ], text),

            "supplier": find_supplier_name(text),

            "customer": labeled_value(

                r"billed\s+to|bill\s+to|customer|client", text

            ),

            "invoice_date": labeled_value(

                r"invoice\s+date|date\s+issued|issue\s+date", text

            ),

            "due_date": labeled_value(

                r"due\s+date|payment\s+due", text

            ),

            "subtotal": find_invoice_amount(text, [

                r"^\s*sub\s*total\b"

            ]),

            "discount": find_value([

                r"^[ \t]*(?:discount|loyalty\s+discount)"

                r"[^\r\n]*?(-\s*[$€£₹]?\s*[\d,]+\.\d{2})"

            ], text),

            "tax": find_invoice_amount(text, [

                r"^\s*(?:sales\s+tax|tax\s+amount|tax|vat|gst)\b"

            ]),

            "total": find_invoice_amount(text, [

                r"^\s*grand\s+total\b",

                r"^\s*total\b",

                r"^\s*balance\s+due\b",

                r"^\s*amount\s+(?:due|payable)\b"

            ])

        }

        required_fields = [

            "invoice_number", "customer", "invoice_date",

            "due_date", "subtotal", "tax", "total"

        ]

        # Validate invoice arithmetic when all relevant values exist.

        subtotal = parse_amount(fields["subtotal"])

        tax = parse_amount(fields["tax"])

        total = parse_amount(fields["total"])

        discount = parse_amount(fields["discount"]) or 0

        if subtotal is not None and tax is not None and total is not None:

            expected_total = subtotal - abs(discount) + tax

            if abs(expected_total - total) > 0.02:

                validation_warnings.append(

                    "Invoice amounts do not reconcile. "

                    "Check subtotal, discount, tax and total."

                )

    elif document_type == "receipt":

        lines = [

            line.strip()

            for line in text.splitlines()

            if line.strip()

        ]

        merchant = labeled_value(

            r"merchant|store|business\s+name", text

        )

        if not merchant and lines:

            merchant = lines[0]

        fields = {

            "merchant": merchant,

            "receipt_number": find_value([

                r"^[ \t]*receipt(?:[ \t]*(?:number|no\.?))?"

                r"[ \t]*[:#]?[ \t]*([A-Z0-9][A-Z0-9/-]*)",

                r"^[ \t]*transaction\s+id[ \t]*[:#]?[ \t]*"

                r"([A-Z0-9][A-Z0-9/-]*)"

            ], text),

            "date": find_value([

                r"\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\b",

                r"\b([A-Z][a-z]+\s+\d{1,2},?\s+\d{4})\b"

            ], text),

            "subtotal": find_value([

                r"^[ \t]*sub[ \t]*total[ \t]*[: ]*[ \t]*"

                r"(?:\r?\n[ \t]*)?"

                r"([$€£₹]?\s*[\d,]+\.\d{2})"

            ], text),

            "tax": find_value([

                r"^[ \t]*(?:tax|vat|gst)"

                r"[ \t]*(?:\(\s*\d+(?:\.\d+)?%\s*\))?"

                r"[ \t]*[:=]?[ \t]*"

                r"(?:\d+(?:\.\d+)?%[ \t]*)?"

                r"(?:\r?\n[ \t]*)?"

                r"([$€£₹]?\s*[\d,]+\.\d{2})",

                r"^[ \t]*incl\.?\s+(?:tax|vat|gst)"

                r"[ \t]+\d+(?:\.\d+)?%[ \t]*\r?\n"

                r"[ \t]*([$€£₹]?\s*[\d,]+\.\d{2})"

            ], text),

            "total": find_value([

                r"^[ \t]*(?:grand\s+total|total\s+paid|"

                r"amount\s+paid|total)"

                r"[ \t]*:?[ \t]*"

                r"(?:\r?\n[ \t]*)?"

                r"([$€£₹]?\s*[\d,]+\.\d{2})"

            ], text)

        }

        required_fields = ["merchant", "total"]

    elif document_type == "resume":

        lines = [

            line.strip()

            for line in text.splitlines()

            if line.strip()

        ]

        fields = {

            "candidate_name": lines[0] if lines else None,

            "email": find_value([

                r"\b([A-Z0-9.\_%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})\b"

            ], text),

            "phone": find_value([

                r"(?:phone|mobile|contact)\s*:\s*"

                r"([+()\d][+\d() .-]{6,}\d)"

            ], text),

            "skills": labeled_value(

                r"technical\s+skills|skills", text

            ),

            "education": labeled_value(

                r"education|academic\s+background", text

            )

        }

        required_fields = ["candidate_name"]

    elif document_type == "contract":

        fields = {

            "party_one": labeled_value(

                r"party\s+one|first\s+party|"

                r"licensor|landlord|lessor", text

            ),

            "party_two": labeled_value(

                r"party\s+two|second\s+party|"

                r"licensee|tenant|lessee", text

            ),

            "effective_date": find_value([

                r"(?:effective|commencement|start)\s+date"

                r"\s*:\s*([^\r\n]+)",

                r"beginning\s+on\s+"

                r"(\d{1,2}\s+[A-Z][a-z]+\s+\d{4})"

            ], text),

            "expiry_date": find_value([

                r"(?:expiry|expiration|end)\s+date"

                r"\s*:\s*([^\r\n]+)",

                r"ending\s+on\s+"

                r"(\d{1,2}\s+[A-Z][a-z]+\s+\d{4})"

            ], text)

        }

        required_fields = ["party_one", "party_two"]

    # Other document types are classified, but field extraction

    # for those types has not been implemented yet.

    else:

        required_fields = []

    fields = {

        key: value.strip()

        for key, value in fields.items()

        if isinstance(value, str) and value.strip()

    }

    missing_required_fields = [

        field for field in required_fields

        if not fields.get(field)

    ]

    if is_ocr:

        validation_warnings.append(

            "OCR was used. Verify extracted values against "

            "the original document, especially amounts and dates."

        )

    if document_type == "invoice" and not fields.get("supplier"):

        validation_warnings.append(

            "Supplier was not explicitly identified by a recognized label. "

            "Verify it manually; the customer is not assumed to be the supplier."

        )

    if document_type == "receipt" and not fields.get("subtotal"):

        validation_warnings.append(

            "Subtotal was not explicitly identified. "

            "It has not been guessed or calculated."

        )

    review_required = (

        document_type == "unknown"

        or bool(missing_required_fields)

        or bool(validation_warnings)

        or document_type in {

            "bank_statement",

            "maintenance_notice",

            "assignment"

        }

    )

    return {

        "document_type": document_type,

        "classification_scores": classification[

            "classification_scores"

        ],

        "fields": fields,

        "missing_required_fields": missing_required_fields,

        "validation_warnings": validation_warnings,

        "review_required": review_required

    }

# -----------------------------

# 5. DOCUMENT PROCESSING

# -----------------------------

def process_document(filename: str):
    """Extract one document; atomically claim it to prevent concurrent processing."""
    now = datetime.now(timezone.utc)
    with documents_transaction() as documents:
        if filename not in documents:
            raise FileNotFoundError(f"No metadata record exists for {filename}")
        record = documents[filename]
        current_status = record.get("processing_status")
        if current_status in {"completed", "needs_ocr"}:
            return current_status
        if current_status == "processing":
            started = record.get("processing_started_at")
            try:
                started_at = datetime.fromisoformat(started.replace("Z", "+00:00")) if started else None
            except (TypeError, ValueError):
                started_at = None
            if started_at and (now - started_at).total_seconds() < PROCESSING_STALE_SECONDS:
                return "processing"
        record["processing_status"] = "processing"
        record["processing_started_at"] = now.isoformat()
        record.pop("processing_error", None)

    try:
        file_path = UPLOAD_FOLDER / filename
        if not file_path.is_file():
            # S3 is the durable original store; restore a local working copy if needed.
            with documents_transaction() as documents:
                record = documents.get(filename)
                if not record:
                    raise FileNotFoundError(f"No metadata record exists for {filename}")
                bucket = record.get("s3_bucket", S3_BUCKET)
                key = record.get("s3_key", f"documents/{filename}")
            file_path.parent.mkdir(parents=True, exist_ok=True)
            s3_client.download_file(bucket, key, str(file_path))

        extracted_text = ""
        extraction_method = "unknown"
        if file_path.suffix.lower() == ".pdf":
            reader = PdfReader(str(file_path))
            extracted_text = "\n".join(page.extract_text() or "" for page in reader.pages).strip()
            if len(extracted_text) < 50:
                pages = convert_from_path(
                    str(file_path), poppler_path=POPPLER_PATH,
                    first_page=1, last_page=10
                )
                extracted_text = "\n".join(
                    pytesseract.image_to_string(page, lang="eng").strip()
                    for page in pages
                ).strip()
                extraction_method = "tesseract_ocr_pdf"
            else:
                extraction_method = "pdf_text"
        else:
            with Image.open(file_path) as image_file:
                extracted_text = pytesseract.image_to_string(image_file, lang="eng").strip()
            extraction_method = "tesseract_ocr"

        if extracted_text:
            structured_data = extract_structured_data(extracted_text, extraction_method)
        else:
            structured_data = {
                "document_type": "unknown",
                "classification_scores": {},
                "fields": {},
                "missing_required_fields": [],
                "validation_warnings": [
                    "No text could be extracted. Check the file quality or OCR configuration."
                ],
                "review_required": True
            }

        with documents_transaction() as documents:
            if filename not in documents:
                raise FileNotFoundError(f"Metadata record disappeared for {filename}")
            documents[filename]["extracted_text"] = extracted_text
            documents[filename]["extraction_method"] = extraction_method
            documents[filename]["structured_data"] = structured_data
            documents[filename]["processing_status"] = "completed" if extracted_text else "needs_ocr"
            documents[filename].pop("processing_started_at", None)
            documents[filename].pop("processing_error", None)
        return "completed" if extracted_text else "needs_ocr"

    except Exception as error:
        try:
            with documents_transaction() as documents:
                if filename in documents:
                    documents[filename]["processing_status"] = "failed"
                    documents[filename].pop("processing_started_at", None)
                    documents[filename]["processing_error"] = str(error)
        except Exception:
            # Preserve the original processing exception for the worker log.
            pass
        raise

# 6. API ENDPOINTS

# -----------------------------

@app.get("/")

def home():

    return {

        "message": "SmartDocs AI API is running!",

        "status": "success"

    }

@app.post("/documents")
async def upload_document(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    original_filename = file.filename or "unknown"
    extension = Path(original_filename).suffix.lower()
    allowed_extensions = {".pdf", ".png", ".jpg", ".jpeg"}
    if extension not in allowed_extensions:
        raise HTTPException(status_code=400, detail="Only PDF and image files are allowed.")

    contents = await file.read()
    await file.close()
    if not contents:
        raise HTTPException(status_code=400, detail="File is empty.")
    if len(contents) > MAX_FILE_SIZE:
        raise HTTPException(status_code=413, detail="File must be 10 MB or smaller.")

    UPLOAD_FOLDER.mkdir(parents=True, exist_ok=True)
    saved_filename = f"{uuid.uuid4().hex}{extension}"
    local_file_path = UPLOAD_FOLDER / saved_filename
    s3_key = f"documents/{saved_filename}"
    content_types = {
        ".pdf": "application/pdf", ".png": "image/png",
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg"
    }

    # Store original in S3 and keep a local processing copy.

    # Store original in S3 using a fresh AWS session.
    try:
        upload_session = boto3.Session(region_name=AWS_REGION)
        upload_s3 = upload_session.client("s3")

        upload_s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=contents,
            ContentType=content_types[extension]
        )

    except (BotoCoreError, ClientError) as error:
        print(
            f"S3 UPLOAD FAILED: {type(error).__name__}: {error}",
            flush=True
        )

        error_code = (
            error.response.get("Error", {}).get("Code", "AWS_ERROR")
            if isinstance(error, ClientError)
            else type(error).__name__
        )

        raise HTTPException(
            status_code=502,
            detail=f"S3 upload failed ({error_code}). Check API logs."
        ) from error



    # Save metadata BEFORE dispatching processing, so any worker can see it.
    try:
        with documents_transaction() as documents:
            documents[saved_filename] = {
                "original_filename": original_filename,
                "size_bytes": len(contents),
                "uploaded_at": datetime.now(timezone.utc).isoformat(),
                "processing_status": "queued",
                "storage": "s3",
                "s3_bucket": S3_BUCKET,
                "s3_key": s3_key,
                "extracted_text": "",
                "extraction_method": None,
                "structured_data": None,
            }
    except Exception as error:
        raise HTTPException(
            status_code=500,
            detail="Document was uploaded to S3, but metadata could not be saved.",
        ) from error


    if PROCESSING_MODE == "local":
        if background_tasks is None:
            raise HTTPException(status_code=500, detail="Background processing is unavailable.")
        background_tasks.add_task(process_document, saved_filename)
    elif PROCESSING_MODE == "sqs":
        try:
            sqs_client.send_message(
                QueueUrl=SQS_QUEUE_URL,
                MessageBody=json.dumps({"filename": saved_filename})
            )
            with documents_transaction() as documents:
                if saved_filename in documents:
                    documents[saved_filename]["queue_last_enqueued_at"] = datetime.now(timezone.utc).isoformat()
        except (BotoCoreError, ClientError) as error:
            with documents_transaction() as documents:
                if saved_filename in documents:
                    documents[saved_filename]["processing_status"] = "queue_failed"
                    documents[saved_filename]["processing_error"] = str(error)
            raise HTTPException(
                status_code=502,
                detail="Document was saved, but the processing job could not be queued.") from error
    else:
        with documents_transaction() as documents:
            documents[saved_filename]["processing_status"] = "queue_failed"
            documents[saved_filename]["processing_error"] = f"Invalid PROCESSING_MODE: {PROCESSING_MODE}"
        raise HTTPException(status_code=500, detail="Invalid processing mode configuration.")

    return {
        "message": "Document uploaded successfully!",
        "saved_filename": saved_filename,
        "processing_status": "queued",
        "message_detail": "Document queued for background processing."
    }


@app.get("/documents")
def list_documents():
    with documents_transaction() as documents:
        for doc in documents.values():
            extracted_text = doc.get("extracted_text", "")
            if not isinstance(extracted_text, str) or not extracted_text.strip():
                continue
            existing = doc.get("structured_data")
            should_rebuild = (
                not isinstance(existing, dict)
                or existing.get("document_type") == "invoice"
            )
            if should_rebuild:
                doc["structured_data"] = extract_structured_data(
                    extracted_text, doc.get("extraction_method", "pdf_text")
                )
    return documents

@app.get("/documents/{filename}/file")

def view_original_document(filename: str):

    if not filename or Path(filename).name != filename:

        raise HTTPException(

            status_code=400,

            detail="Invalid filename."

        )

    documents = load_documents()

    if filename not in documents:

        raise HTTPException(

            status_code=404,

            detail="Document not found."

        )

    file_path = UPLOAD_FOLDER / filename

    if not file_path.is_file():

        raise HTTPException(

            status_code=404,

            detail="Original document file not found."

        )

    media_types = {

        ".pdf": "application/pdf",

        ".png": "image/png",

        ".jpg": "image/jpeg",

        ".jpeg": "image/jpeg"

    }

    media_type = media_types.get(file_path.suffix.lower())

    if not media_type:

        raise HTTPException(

            status_code=415,

            detail="Unsupported document type."

        )

    original_filename = documents[filename].get(

        "original_filename", filename

    )

    return FileResponse(

        path=file_path,

        media_type=media_type,

        filename=original_filename,

        content_disposition_type="inline"

    )

@app.get("/documents/{filename}")

def get_document_status(filename: str):

    if not filename or Path(filename).name != filename:

        raise HTTPException(

            status_code=400,

            detail="Invalid filename."

        )

    documents = load_documents()

    if filename not in documents:

        raise HTTPException(

            status_code=404,

            detail="Document not found."

        )

    return documents[filename]
