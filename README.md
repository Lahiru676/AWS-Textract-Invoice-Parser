# AWS-Textract-Invoice-Parser
Extracts structured invoice data (header fields + line items) from PDFs and images using AWS Textract – AnalyzeExpense, with a FORMS fallback for missing headers. Handles multi-page invoices, merges page results, and prints a clean console report plus normalized JSON.
✨ What it does

Accepts files from your local drive (.pdf, .jpg, .jpeg, .png)

Uploads to your S3 bucket and runs Textract AnalyzeExpense (async)

(If needed) runs Textract FORMS to recover header KV pairs

Merges multi-page outputs, picks the primary doc, and sanitizes line items

Resolves tricky cases where unit price ↔ amount columns are swapped

Prints a pretty table to the console and writes:

*.textract_raw.json – raw Textract pages

*.parsed.json – parsed/merged docs

*_clean.json – final normalized object per assignment spec

🧱 Project layout
.
├─ main.py                 # Orchestrator / CLI
├─ client.py               # AWS clients, S3 upload, Textract job helpers
├─ expense_parser.py       # Parse AnalyzeExpense & FORMS; line-item logic
├─ aggregater.py           # Merge docs by invoice number (if used)
├─ utils.py                # Dates, currency/qty parsing, JSON utils, env helpers
└─ .env                    # Your configuration (not committed)


Your instructor will grade API handling; that happens in client.py (S3 + Textract async APIs) and in main.py (orchestration).

⚙️ Requirements

Python 3.9+ recommended

An AWS account with permissions for:

textract:StartExpenseAnalysis, textract:GetExpenseAnalysis

textract:StartDocumentAnalysis, textract:GetDocumentAnalysis

s3:PutObject, s3:GetObject on your bucket

Install deps:

python -m venv env
# Windows:   env\Scripts\activate
# macOS/Linux: source env/bin/activate
pip install -r requirements.txt


requirements.txt (create this file):

boto3
python-dotenv
python-dateutil

🔐 .env configuration

Create a .env in the project root:

# AWS & S3
AWS_REGION=us-east-1
S3_BUCKET=your-s3-bucket-name
S3_PREFIX=invoices/          # optional; can be empty
POLL_SECS=4                  # polling interval for async jobs (seconds)

# Parsing defaults
DEFAULT_CURRENCY=USD
DATE_DAYFIRST=true           # parse day-first dates common outside US (true/false)

# (Use standard AWS credentials chain: env vars, shared profile, or role)
# Example (optional if you already configured AWS CLI/role):
# AWS_ACCESS_KEY_ID=...
# AWS_SECRET_ACCESS_KEY=...
# AWS_SESSION_TOKEN=...


Do not commit .env. Let your AWS credentials be picked up from your normal profile or role when possible.

▶️ Running

Process one or more files from your local drive:

python main.py "C:\path\to\invoice 01.pdf" "C:\path\to\invoice 05.pdf"
# or
python main.py ./samples/invoice01.pdf ./samples/invoice05.png





Artifacts saved next to your input:

invoice_01.textract_raw.json

invoice_01.parsed.json

invoice_01_clean.json

🔍 How it works (pipeline)

Local file → S3
client.upload_local_*_to_s3() uploads the file with a unique key (prefix + UUID).

AnalyzeExpense (async)
client.start_expense_job() → poll with wait_for_job() → fetch paginated results with fetch_all_pages().

Parse
expense_parser.parse_expense_documents() walks ExpenseDocuments:

Extracts header fields (invoice number/date/total/terms) from Textract types and labels

Extracts line items and chooses the best (unitPrice, amount) pair using quantity-aware scoring with soft hints and a safety swap if columns were reversed

Filters per-row values that simply repeat the invoice total

Merge pages / choose primary
If multiple docs are returned, aggregater.merge_documents_by_invoice_number() (if used) merges them; then choose_primary_document() selects a final representative.

FORMS fallback
If a header field is missing, StartDocumentAnalysis(FORMS) runs once and parse_forms_key_values() helps fill gaps (invoice number/date/terms).

Sanitize & print
sanitize_line_items() drops summary rows (Subtotal, Tax, etc.), fixes zero-qty noise, and computes missing fields only when they are actually missing (never overwriting values Textract provided).
main.print_invoice_report() displays the table; utils.save_json() writes artifacts.

📄 Supported inputs

PDF (multi-page supported)

JPG / JPEG / PNG (single or multi-page image sets as separate files)



🧩 Extending

Add vendor-specific rules in expense_parser.py if a known template always mislabels headers.

Emit CSV alongside JSON (e.g., one row per line item).

Add S3 cleanup (optional) after processing.
