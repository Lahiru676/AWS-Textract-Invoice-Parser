import sys, json
from pathlib import Path
from decimal import Decimal
from typing import Any, Dict, List, Optional

from client import (
    load_config, get_s3_client, get_textract_client,
    upload_local_pdf_to_s3, start_expense_job, wait_for_job, fetch_all_pages,
    start_forms_job, wait_for_forms_job, fetch_all_pages_forms
)
from expense_parser import (
    parse_expense_documents, parse_forms_key_values,
    choose_primary_document, sanitize_line_items,
    RE_INV_NUM, RE_INV_DATE, RE_TERMS
)
from aggregater import merge_documents_by_invoice_number
from utils import (
    save_json, clean_text, normalize_date, detect_currency_hint,
    pretty_money, to_decimal_maybe, is_currency_like, get_default_currency
)

def _extract_from_kv(kv: Dict[str, str], patterns) -> Optional[str]:
    for k, v in kv.items():
        for rx in patterns:
            if rx.search(k):
                return v
    return None

def print_invoice_report(inv: Dict[str, Any], currency_hint="USD"):
    inv_no   = clean_text(inv.get("invoiceNumber")) or "-"
    inv_date = normalize_date(inv.get("invoiceDate")) or "-"
    payterms = clean_text(inv.get("paymentTerms")) or "-"
    total_d  = to_decimal_maybe(inv.get("total"))
    items    = sanitize_line_items(inv.get("lineItems", []))
    items    = [x for x in items if any([x["description"], x["quantity"], x["unitPrice"], x["amount"]])]

    subtotal = sum([x["amount"] for x in items if x["amount"] is not None], start=Decimal("0"))
    inferred = sum(1 for x in items if x["amount"] is not None and (x["quantity"] is not None and x["unitPrice"] is not None))

    print("\n================ INVOICE =================")
    print(f"Invoice Number : {inv_no}")
    print(f"Invoice Date   : {inv_date}")
    print(f"Payment Terms  : {payterms}")
    print("---------------------------------------------------------------")
    print(f"{'Description':40} {'Qty':>8} {'Unit Price':>14} {'Amount':>14}")
    print("---------------------------------------------------------------")
    for it in items:
        desc = (it['description'] or '-')[:40]
        qty  = f"{it['quantity']}" if it['quantity'] is not None else "-"
        unit = pretty_money(it['unitPrice'], currency_hint) if it['unitPrice'] is not None else "-"
        amt  = pretty_money(it['amount'],     currency_hint) if it['amount']     is not None else "-"
        print(f"{desc:40} {qty:>8} {unit:>14} {amt:>14}")
    print("---------------------------------------------------------------")
    print(f"{'Invoice Total:':>54} {pretty_money(total_d, currency_hint):>14}")
    
    print("===============================================================\n")

def make_clean_json(inv: Dict[str, Any]) -> Dict[str, Any]:
    from expense_parser import sanitize_line_items
    from utils import to_decimal_maybe
    items = sanitize_line_items(inv.get("lineItems", []))
    def dec_to_str(d: Optional[Decimal]) -> Optional[str]:
        return f"{d:.2f}" if d is not None else None
    return {
        "invoiceNumber": clean_text(inv.get("invoiceNumber")),
        "invoiceDate":   normalize_date(inv.get("invoiceDate")),
        "paymentTerms":  clean_text(inv.get("paymentTerms")),
        "lineItems": [
            {
                "description": it["description"],
                "quantity":    f"{it['quantity']}" if it["quantity"] is not None else None,
                "unitPrice":   dec_to_str(it["unitPrice"]),
                "amount":      dec_to_str(it["amount"]),
            }
            for it in items
        ],
        "total": (f"{to_decimal_maybe(inv.get('total')):.2f}" if to_decimal_maybe(inv.get("total")) is not None else None)
    }

def process_local_pdf(local_pdf_path: str, cfg) -> Dict[str, Any]:
    bucket, prefix, poll = cfg["S3_BUCKET"], cfg["S3_PREFIX"], cfg["POLL_SECS"]
    region = cfg["REGION"]

    s3       = get_s3_client(region)
    textract = get_textract_client(region)

    # 1) upload to S3
    s3_key = upload_local_pdf_to_s3(local_pdf_path, bucket, prefix, s3)

    # 2) start Expense job
    job_id = start_expense_job(bucket, s3_key, textract)
    print(f"Started Expense job: {job_id} for s3://{bucket}/{s3_key}")

    # 3) wait + fetch pages
    status = wait_for_job(job_id, textract, poll)
    if status not in ("SUCCEEDED", "PARTIAL_SUCCESS"):
        print(f"[!] Job did not succeed: {status}")
        return {"jobId": job_id, "status": status, "s3Key": s3_key}

    pages = fetch_all_pages(job_id, textract)

    # 4) save raw + parsed
    base_name = Path(local_pdf_path).stem.replace(" ", "_")
    save_json(pages, f"{base_name}.textract_raw.json")

    initial_parsed = parse_expense_documents(pages)
    merged_docs    = merge_documents_by_invoice_number(initial_parsed)
    save_json(merged_docs, f"{base_name}.parsed.json")

    # 5) pick primary and fallback with FORMS if needed
    primary = choose_primary_document(merged_docs)
    if not primary:
        print(f"[!] No usable invoice found in {local_pdf_path}")
        return {"jobId": job_id, "status": status, "s3Key": s3_key, "parsed_docs": merged_docs}

    need_inv = (not primary.get("invoiceNumber")) or is_currency_like(primary.get("invoiceNumber"))
    need_dt  = not primary.get("invoiceDate")
    need_trm = not primary.get("paymentTerms")

    if need_inv or need_dt or need_trm:
        fj = start_forms_job(bucket, s3_key, textract)
        fstatus = wait_for_forms_job(fj, textract, poll)
        if fstatus in ("SUCCEEDED", "PARTIAL_SUCCESS"):
            fpages = fetch_all_pages_forms(fj, textract)
            kv = parse_forms_key_values(fpages)
            if need_inv:
                inv_no = _extract_from_kv(kv, RE_INV_NUM)
                if inv_no and not is_currency_like(inv_no): primary["invoiceNumber"] = inv_no
            if need_dt:
                inv_dt = _extract_from_kv(kv, RE_INV_DATE)
                if inv_dt: primary["invoiceDate"] = inv_dt
            if need_trm:
                inv_terms = _extract_from_kv(kv, RE_TERMS)
                if inv_terms: primary["paymentTerms"] = inv_terms

    currency_hint = detect_currency_hint(primary) or get_default_currency()

    print_invoice_report(primary, currency_hint=currency_hint)

    clean_obj  = make_clean_json(primary)
    clean_path = f"{base_name}_clean.json"
    save_json(clean_obj, clean_path)

    return {
        "jobId": job_id,
        "status": status,
        "s3Key": s3_key,
        "clean_json": clean_path
    }

if __name__ == "__main__":
    try:
        cfg = load_config()
    except Exception as e:
        print(f"[CONFIG ERROR] {e}")
        sys.exit(1)

    if len(sys.argv) < 2:
        print("Usage: python main.py <invoice.pdf> [<invoice2.pdf> ...]")
        sys.exit(1)

    outputs = []
    for local in sys.argv[1:]:
        print(f"\n=== Processing: {local} ===")
        try:
            out = process_local_pdf(local, cfg)
        except Exception as e:
            out = {"status":"ERROR","message":str(e), "file": local}
            print(f"[ERROR] {e}")
        outputs.append(out)

    print("\n=== SUMMARY ===")
    print(json.dumps(outputs, indent=2))
