import os, uuid, mimetypes, time
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

import boto3
from dotenv import load_dotenv

#  env / clients 

def load_config():
    load_dotenv()
    cfg = {
        "REGION": os.getenv("AWS_REGION", "us-east-1"),
        "S3_BUCKET": os.getenv("S3_BUCKET"),
        "S3_PREFIX": os.getenv("S3_PREFIX", "invoices/"),
        "POLL_SECS": float(os.getenv("POLL_SECS", "4")),
    }
    if not cfg["S3_BUCKET"]:
        raise ValueError("S3_BUCKET is not set.")
    return cfg

def get_textract_client(region: str):
    return boto3.client("textract", region_name=region)

def get_s3_client(region: str):
    return boto3.client("s3", region_name=region)

#  S3 helpers 

# Allowed local input extensions
ALLOWED_EXTS = {".pdf", ".png", ".jpg", ".jpeg", ".tif", ".tiff"}

# Async (StartExpenseAnalysis) supports only PDF/TIFF
ASYNC_EXTS = {".pdf", ".tif", ".tiff"}

def _guess_content_type(path: Path) -> str:
    ctype, _ = mimetypes.guess_type(str(path))
    return ctype or "application/octet-stream"

def _make_s3_key_from_local(path: Path, prefix: str = "") -> str:
    base = path.name.replace(" ", "_")
    uid  = uuid.uuid4().hex[:8]
    return f"{prefix}{uid}-{base}" if prefix else f"{uid}-{base}"

def upload_local_file_to_s3(local_path: str, bucket: str, prefix: str, s3_client) -> str:
    """
    Upload PDF/PNG/JPG/TIFF to S3 and return the S3 key.
    (Replaces PDF-only behavior; supports images too.)
    """
    p = Path(local_path).expanduser().resolve()
    if not p.exists() or not p.is_file():
        raise FileNotFoundError(f"File not found: {local_path}")
    if p.suffix.lower() not in ALLOWED_EXTS:
        raise ValueError(f"Only {sorted(ALLOWED_EXTS)} supported. Got: {p.suffix}")
    key = _make_s3_key_from_local(p, prefix)
    print(f"Uploading {p.name} → s3://{bucket}/{key}")
    s3_client.upload_file(str(p), bucket, key, ExtraArgs={"ContentType": _guess_content_type(p)})
    return key


def upload_local_pdf_to_s3(local_path: str, bucket: str, prefix: str, s3_client) -> str:
    return upload_local_file_to_s3(local_path, bucket, prefix, s3_client)

def _is_async_supported_ext(key: str) -> bool:
    return Path(key).suffix.lower() in ASYNC_EXTS

#  Textract (Expense) APIs 

# ASYNC (PDF/TIFF)
def start_expense_job(bucket: str, key: str, textract) -> str:
    r = textract.start_expense_analysis(DocumentLocation={"S3Object": {"Bucket": bucket, "Name": key}})
    return r["JobId"]

def wait_for_job(job_id: str, textract, poll_secs: float) -> str:
    while True:
        r = textract.get_expense_analysis(JobId=job_id)
        status = r["JobStatus"]
        print(f"[{job_id}] Status: {status}")
        if status in ("SUCCEEDED", "FAILED", "PARTIAL_SUCCESS"):
            return status
        time.sleep(poll_secs)

def fetch_all_pages(job_id: str, textract) -> List[Dict[str, Any]]:
    pages, token = [], None
    while True:
        r = textract.get_expense_analysis(JobId=job_id, NextToken=token) if token else textract.get_expense_analysis(JobId=job_id)
        pages.append(r)
        token = r.get("NextToken")
        if not token:
            break
        time.sleep(0.2)
    return pages

# SYNC (works for PNG/JPG/TIFF/PDF)
def analyze_expense_s3object(bucket: str, key: str, textract):
    """
    Synchronous AnalyzeExpense on an S3 object (use for images like .jpg/.png).
    Returns the raw AnalyzeExpense response.
    """
    return textract.analyze_expense(Document={"S3Object": {"Bucket": bucket, "Name": key}})

#  auto-select async/sync based on file type.
def analyze_expense_auto(
    bucket: str,
    key: str,
    textract,
    poll_secs: float
) -> Tuple[str, str, List[Dict[str, Any]]]:
    """
    Decide the right API based on extension.
    - For .pdf/.tif/.tiff: StartExpenseAnalysis (async) + polling + pagination
    - For .jpg/.jpeg/.png: AnalyzeExpense (sync)
    Returns: (job_id, status, pages_as_list)
      - For sync: job_id="SYNC", status="SUCCEEDED", pages=[{"ExpenseDocuments":[...]}]
      - For async: job_id=<uuid>, status=<final>, pages=[ ... paginated responses ... ]
    """
    if _is_async_supported_ext(key):
        job_id = start_expense_job(bucket, key, textract)
        print(f"Started Expense job: {job_id} for s3://{bucket}/{key}")
        status = wait_for_job(job_id, textract, poll_secs)
        if status not in ("SUCCEEDED", "PARTIAL_SUCCESS"):
            return job_id, status, []
        pages = fetch_all_pages(job_id, textract)
        return job_id, status, pages
    else:
        resp = analyze_expense_s3object(bucket, key, textract)
        pages = [{"ExpenseDocuments": resp.get("ExpenseDocuments", [])}]
        return "SYNC", "SUCCEEDED", pages

#  Textract async (FORMS) for fallback 

def start_forms_job(bucket: str, key: str, textract) -> str:
    r = textract.start_document_analysis(
        DocumentLocation={"S3Object": {"Bucket": bucket, "Name": key}},
        FeatureTypes=["FORMS"]
    )
    return r["JobId"]

def wait_for_forms_job(job_id: str, textract, poll_secs: float) -> str:
    while True:
        r = textract.get_document_analysis(JobId=job_id)
        status = r["JobStatus"]
        print(f"[FORMS {job_id}] Status: {status}")
        if status in ("SUCCEEDED", "FAILED", "PARTIAL_SUCCESS"):
            return status
        time.sleep(poll_secs)

def fetch_all_pages_forms(job_id: str, textract) -> List[Dict[str, Any]]:
    pages, token = [], None
    while True:
        r = textract.get_document_analysis(JobId=job_id, NextToken=token) if token else textract.get_document_analysis(JobId=job_id)
        pages.append(r)
        token = r.get("NextToken")
        if not token:
            break
        time.sleep(0.2)
    return pages
