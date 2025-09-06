import re
from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal
from utils import (
    clean_text, normalize_date, to_decimal_maybe, to_decimal_qty,
    compute_amount_if_missing, compute_qty_if_missing, is_currency_like
)

# header label regex 
LABEL_FLAGS = re.IGNORECASE
RE_INV_NUM   = [re.compile(r"\b(invoice|inv)\s*(no\.?|number|#)\b", LABEL_FLAGS)]
RE_INV_DATE  = [re.compile(r"\binvoice\s*date\b", LABEL_FLAGS)]
RE_TOTAL     = [re.compile(r"\b(grand\s+)?total\b", LABEL_FLAGS),
                re.compile(r"\btotal\s+amount\b", LABEL_FLAGS),
                re.compile(r"\bamount\s+due\b", LABEL_FLAGS)]
RE_TERMS     = [re.compile(r"\bpayment\s*terms\b", LABEL_FLAGS),
                re.compile(r"\bpayment\s*due\b", LABEL_FLAGS),
                re.compile(r"\bterms\b", LABEL_FLAGS)]

# FORMS parsing (fallback) 
def _get_text_from_block(block: Dict[str, Any], id_map: Dict[str, Dict[str, Any]]) -> str:
    parts: List[str] = []
    for rel in block.get("Relationships", []) or []:
        if rel.get("Type") == "CHILD":
            for cid in rel.get("Ids", []):
                cb = id_map.get(cid, {})
                if cb.get("BlockType") == "WORD":
                    parts.append(cb.get("Text", ""))
                elif cb.get("BlockType") == "SELECTION_ELEMENT" and cb.get("SelectionStatus") == "SELECTED":
                    parts.append("X")
    return " ".join([t for t in parts if t]).strip()

def parse_forms_key_values(pages: List[Dict[str, Any]]) -> Dict[str, str]:
    kv: Dict[str, str] = {}
    for page in pages:
        blocks = page.get("Blocks", []) or []
        id_map = {b["Id"]: b for b in blocks if "Id" in b}
        for b in blocks:
            if b.get("BlockType") == "KEY_VALUE_SET" and "KEY" in (b.get("EntityTypes") or []):
                key_txt = _get_text_from_block(b, id_map)
                value_txt = ""
                for rel in b.get("Relationships", []) or []:
                    if rel.get("Type") == "VALUE":
                        for vid in rel.get("Ids", []):
                            vb = id_map.get(vid, {})
                            value_txt = _get_text_from_block(vb, id_map) or value_txt
                key_norm = (key_txt or "").strip().lower()
                val_norm = (value_txt or "").strip()
                if key_norm:
                    if key_norm not in kv or not kv[key_norm]:
                        kv[key_norm] = val_norm
    return kv

#  Expense helpers 
def get_summary_value_by_type(summary_fields: List[Dict[str, Any]], type_name: str) -> Optional[str]:
    for f in summary_fields:
        if (f.get("Type") or {}).get("Text") == type_name:
            vd = f.get("ValueDetection") or {}
            text = vd.get("Text")
            if text:
                return text
    return None

def get_summary_value_by_label(summary_fields: List[Dict[str, Any]], label_regexes: List[re.Pattern]) -> Optional[str]:
    for f in summary_fields:
        lbl = ((f.get("LabelDetection") or {}).get("Text") or "").strip()
        val = (f.get("ValueDetection") or {}).get("Text")
        if not lbl or not val:
            continue
        lbl_norm = lbl.lower()
        for rx in label_regexes:
            if rx.search(lbl_norm):
                return val
    return None

#  helpers for candidate scoring 
def _close(a: Optional[Decimal], b: Optional[Decimal]) -> bool:
    if a is None or b is None:
        return False
    tol = max((b.copy_abs() * Decimal("0.02")), Decimal("0.05"))  # 2% or 0.05
    return (a - b).copy_abs() <= tol

def _push_unique(lst: List[str], v: Optional[str]):
    if not v:
        return
    v2 = v.strip()
    if v2 and v2 not in lst:
        lst.append(v2)

def _soft_amount_bonus(s: str) -> Decimal:
    bonus = Decimal("0")
    if is_currency_like(s):
        bonus -= Decimal("0.05") 
    d = to_decimal_maybe(s)
    if d is not None:
        # round to nearest cent
        q = (d * 100) % 1
        if q == 0:
            bonus -= Decimal("0.01")
    return bonus

def _soft_rate_bonus(s: str) -> Decimal:
    bonus = Decimal("0")
    if is_currency_like(s):
        bonus += Decimal("0.03")  
    return bonus

#  Expense normalized docs 
def parse_expense_documents(results_pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Normalize Textract Expense output into a list of docs:
    { invoiceNumber, invoiceDate, total, paymentTerms, lineItems: [...] }

    - Uses TYPE-first then LABELs for line item fields
    - Chooses (unitPrice, amount) pair via qty-aware error minimization
    - Filters out row-amounts equal to invoice total (echoed on each row)
    - Only fills missing values (does not overwrite what the PDF already provided)
    - Swaps rate/amount if columns were flipped but qty*rate matches after swapping
    """
    parsed: List[Dict[str, Any]] = []

    QUANTITY_KEYS    = {"QUANTITY", "QTY", "HOURS", "HOUR", "UNITS"}
    UNIT_PRICE_KEYS  = {"UNIT_PRICE", "PRICE", "RATE"}
    AMOUNT_KEYS      = {"AMOUNT", "TOTAL", "LINE_TOTAL", "NET_AMOUNT", "LINE_AMOUNT", "AMOUNT_AFTER_DISCOUNT"}
    DESC_KEYS        = {"ITEM", "DESCRIPTION", "PRODUCT_CODE", "SERVICE"}

    RATE_LABEL_RE   = re.compile(r"\b(rate|unit price|price)\b", re.I)
    AMOUNT_LABEL_RE = re.compile(r"\b(amount|line amount|line total|total)\b", re.I)
    QTY_LABEL_RE    = re.compile(r"\b(hours?|qty|quantity|units?|pcs?)\b", re.I)
    DESC_LABEL_RE   = re.compile(r"\b(description|item|service)\b", re.I)

    def li_extract(fields: List[Dict[str, Any]], total_hint_num: Optional[Decimal]) -> Dict[str, Optional[str]]:
        desc = None
        qty_raw = None
        rate_cands: List[str] = []
        amt_cands:  List[str] = []

        #  TYPE-first collection (priority)
        for f in fields:
            t = ((f.get("Type") or {}).get("Text") or "").upper()
            lbl = ((f.get("LabelDetection") or {}).get("Text") or "").strip()
            val = (f.get("ValueDetection") or {}).get("Text") or ""
            lbl_norm = lbl.lower()

            if not desc and (t in DESC_KEYS or DESC_LABEL_RE.search(lbl_norm)):
                desc = val.strip()

            if (t in QUANTITY_KEYS) or QTY_LABEL_RE.search(lbl_norm):
                if not qty_raw:
                    qty_raw = val

            if t in UNIT_PRICE_KEYS:
                _push_unique(rate_cands, val)
            if t in AMOUNT_KEYS:
                _push_unique(amt_cands, val)

        #  LABEL-based collection (secondary)
        for f in fields:
            lbl = ((f.get("LabelDetection") or {}).get("Text") or "").strip().lower()
            val = (f.get("ValueDetection") or {}).get("Text") or ""
            if RATE_LABEL_RE.search(lbl):
                _push_unique(rate_cands, val)
            if AMOUNT_LABEL_RE.search(lbl):
                _push_unique(amt_cands, val)

        #  Remove row amounts that equal the invoice total (echoed on each row)
        if total_hint_num is not None and amt_cands:
            filtered = []
            for a in amt_cands:
                ad = to_decimal_maybe(a)
                if ad is None or not _close(ad, total_hint_num):
                    filtered.append(a)
            amt_cands = filtered or amt_cands

        #  Choose best pair using qty with soft bonuses
        qd = to_decimal_qty(qty_raw)
        rate = amount = None

        if qd is not None and rate_cands and amt_cands:
            best_pair: Tuple[Optional[str], Optional[str], Decimal] = (None, None, Decimal("1e9"))
            for r in rate_cands:
                rd = to_decimal_maybe(r)
                if rd is None:
                    continue
                for a in amt_cands:
                    ad = to_decimal_maybe(a)
                    if ad is None:
                        continue
                    expected = (qd * rd).quantize(Decimal("0.01"))
                    err = (expected - ad).copy_abs()
                    err += _soft_rate_bonus(r)
                    err += _soft_amount_bonus(a)
                    if qd >= 1 and ad < rd:  # when qty>=1, amount usually >= rate
                        err += Decimal("0.10")
                    if err < best_pair[2]:
                        best_pair = (r, a, err)
            if best_pair[0] is not None:
                rate, amount = best_pair[0], best_pair[1]

        # If still missing, choose first available from candidates
        if rate is None and rate_cands:
            rate = rate_cands[0]
        if amount is None and amt_cands:
            amount = amt_cands[0]

        #  Only compute missing value (do NOT overwrite existing)
        if amount is None:
            comp = compute_amount_if_missing(qty_raw, rate)
            amount = f"{comp}" if comp is not None else None
        if rate is None and qd is not None and amount:
            ad = to_decimal_maybe(amount)
            if ad is not None and qd != 0:
                rate = f"{(ad / qd).quantize(Decimal('0.01'))}"

        # If all present but inconsistent, try swapping rate<->amount
        qd2 = to_decimal_qty(qty_raw)
        rd2 = to_decimal_maybe(rate)
        ad2 = to_decimal_maybe(amount)
        if qd2 is not None and rd2 is not None and ad2 is not None:
            exp = (qd2 * rd2).quantize(Decimal("0.01"))
            if not _close(exp, ad2):
                exp_swapped = (qd2 * ad2).quantize(Decimal("0.01"))
                if _close(exp_swapped, rd2):
                    rate, amount = amount, rate

        return {"description": desc, "quantity": qty_raw, "unitPrice": rate, "amount": amount}

    for page in results_pages:
        for doc in page.get("ExpenseDocuments", []):
            summary_fields   = doc.get("SummaryFields", [])
            line_item_groups = doc.get("LineItemGroups", [])

            def sumval(*alts: str) -> Optional[str]:
                for a in alts:
                    v = get_summary_value_by_type(summary_fields, a)
                    if v: return v
                return None

            invoice_number = sumval("INVOICE_RECEIPT_ID", "INVOICE_NUMBER") or \
                             get_summary_value_by_label(summary_fields, RE_INV_NUM)
            invoice_date   = sumval("INVOICE_RECEIPT_DATE", "INVOICE_DATE") or \
                             get_summary_value_by_label(summary_fields, RE_INV_DATE)
            total_amt      = sumval("TOTAL", "GRAND_TOTAL") or \
                             get_summary_value_by_label(summary_fields, RE_TOTAL)
            payment_terms  = sumval("PAYMENT_TERMS", "TERMS") or \
                             get_summary_value_by_label(summary_fields, RE_TERMS)

            total_hint_num = to_decimal_maybe(total_amt)

            items: List[Dict[str, Any]] = []
            for lig in line_item_groups:
                for li in lig.get("LineItems", []):
                    fields = li.get("LineItemExpenseFields", [])
                    items.append(li_extract(fields, total_hint_num))

            parsed.append({
                "invoiceNumber": invoice_number,
                "invoiceDate":   invoice_date,
                "total":         total_amt,
                "paymentTerms":  payment_terms,
                "lineItems":     items,
            })
    return parsed

def choose_primary_document(docs: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not docs: return None
    with_total = [d for d in docs if to_decimal_maybe(d.get("total")) is not None]
    candidates = with_total if with_total else docs
    candidates.sort(key=lambda d: len([x for x in d.get("lineItems", []) if any(x.values())]), reverse=True)
    return candidates[0]

#  drop summary rows + fix qty=0 noise 
SUMMARY_RX = re.compile(
    r"^\s*(sub\s*total|subtotal|total(?!\s*fee)|tax|vat|discount|fee\s*discount|"
    r"total\s*fees(?!\w)|total\s*disbursements(?!\w))\b",
    re.IGNORECASE
)

def sanitize_line_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cleaned: List[Dict[str, Any]] = []
    for it in items or []:
        desc = clean_text(it.get("description"))
        qty  = clean_text(it.get("quantity"))
        price= clean_text(it.get("unitPrice"))
        amt  = clean_text(it.get("amount"))

        # Drop obvious summary/heading rows
        if desc and SUMMARY_RX.search(desc):
            continue

        qty_d   = to_decimal_qty(qty)
        price_d = to_decimal_maybe(price)
        amt_d   = to_decimal_maybe(amt)

        # Compute missing amount if qty×price present (don't overwrite existing)
        if amt_d is None:
            amt_d = compute_amount_if_missing(qty, price)

        #  If qty parsed as 0 but amount present and no usable unit price, treat qty as unknown
        if qty_d is not None and qty_d == 0 and amt_d not in (None, Decimal("0")) and not price_d:
            qty_d = None

        #  Infer qty from amount & unit price if still missing
        if qty_d is None and amt_d is not None and price_d not in (None, Decimal("0")):
            qd2 = (amt_d / price_d).quantize(Decimal("0.001"))
            if (qd2 - qd2.to_integral_value()).copy_abs() <= Decimal("0.001"):
                qd2 = qd2.to_integral_value()
            qty_d = qd2

        #  Skip empty rows
        if not (desc or qty_d is not None or price_d is not None or amt_d is not None):
            continue

        cleaned.append({
            "description": desc,
            "quantity": qty_d,
            "unitPrice": price_d,
            "amount": amt_d
        })
    return cleaned

# export label regex for fallback in main
__all__ = [
    "parse_expense_documents", "choose_primary_document", "sanitize_line_items",
    "parse_forms_key_values", "RE_INV_NUM", "RE_INV_DATE", "RE_TOTAL", "RE_TERMS"
]
