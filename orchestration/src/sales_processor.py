# Pure Python logic — no Dagster imports here.
# Think of this like your ETL helper functions.

def hello():
    return "Hello from src/sales_processor!"


def step1_extract():
    print("  [step1] Extracting raw sales data...")
    return {"sales_total": 5000, "items_sold": 100}


def step2_transform(raw_data: dict) -> float:
    print("  [step2] Transforming: calculating net revenue after tax...")
    tax = raw_data["sales_total"] * 0.1
    return raw_data["sales_total"] - tax


def step3_load(net_revenue: float):
    print(f"  [step3] Loading net revenue ({net_revenue}) to Data Warehouse.")
