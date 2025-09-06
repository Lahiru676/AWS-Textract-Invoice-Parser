from typing import List, Dict, Any

def merge_documents_by_invoice_number(docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Merge multiple page-level docs into one per invoiceNumber.
    If invoiceNumber missing, put into a special __NO_ID__ bucket.
    Fills missing headers from better-scored pages and concatenates line items.
    """
    if not docs:
        return []
    groups: Dict[str, List[Dict[str, Any]]] = {}
    def key_of(d: Dict[str, Any]) -> str:
        inv = (d.get("invoiceNumber") or "").strip()
        return inv or "__NO_ID__"

    for d in docs:
        groups.setdefault(key_of(d), []).append(d)

    merged: List[Dict[str, Any]] = []
    for inv_no, group in groups.items():
        def score(d: Dict[str, Any]) -> float:
            return sum([
                1 if d.get("total") else 0,
                1 if d.get("invoiceDate") else 0,
                1 if d.get("paymentTerms") else 0,
                len(d.get("lineItems", [])) / 1000.0
            ])
        group_sorted = sorted(group, key=score, reverse=True)
        base = group_sorted[0].copy()
        for other in group_sorted[1:]:
            if not base.get("invoiceDate")   and other.get("invoiceDate"):   base["invoiceDate"] = other["invoiceDate"]
            if not base.get("paymentTerms")  and other.get("paymentTerms"):  base["paymentTerms"] = other["paymentTerms"]
            if not base.get("total")         and other.get("total"):         base["total"] = other["total"]

        all_items: List[Dict[str, Any]] = []
        for d in group_sorted:
            all_items.extend(d.get("lineItems", []))
        base["lineItems"] = all_items
        merged.append(base)
    return merged
