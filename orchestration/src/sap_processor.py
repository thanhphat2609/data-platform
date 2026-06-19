# Pure Python logic for SAP data extraction and processing.

def step1_extract_from_sap() -> list[dict]:
    print("  [step1] Connecting to SAP RFC and extracting purchase orders...")
    return [
        {"po_number": "PO-001", "vendor": "ABC Corp", "amount": 12000.0, "currency": "USD"},
        {"po_number": "PO-002", "vendor": "XYZ Ltd",  "amount":  8500.0, "currency": "USD"},
        {"po_number": "PO-003", "vendor": "DEF Inc",  "amount": 31000.0, "currency": "USD"},
    ]


def step2_validate_and_transform(records: list[dict]) -> list[dict]:
    print("  [step2] Validating fields and converting currency to VND...")
    exchange_rate = 25000
    cleaned = []
    for r in records:
        if not r.get("po_number") or r["amount"] <= 0:
            print(f"  [skip] Invalid record: {r}")
            continue
        cleaned.append({
            **r,
            "amount_vnd": r["amount"] * exchange_rate,
        })
    return cleaned


def step3_load_to_warehouse(records: list[dict]):
    print(f"  [step3] Loading {len(records)} SAP records to Data Warehouse...")
    for r in records:
        print(f"    → {r['po_number']} | {r['vendor']} | {r['amount_vnd']:,.0f} VND")
