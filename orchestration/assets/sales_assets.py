import pandas as pd
from dagster import asset

from src.sales_processor import step1_extract, step2_transform

_QUEUE = {"dagster-celery/queue": "worker-2"}


@asset(
    key_prefix=["sales"],
    group_name="sales",
    op_tags=_QUEUE,
    description="Raw sales data extracted from source system.",
)
def sales_raw() -> pd.DataFrame:
    raw = step1_extract()
    return pd.DataFrame([raw])


@asset(
    key_prefix=["sales"],
    group_name="sales",
    op_tags=_QUEUE,
    description="Net revenue after 10% tax — ready for reporting.",
)
def sales_transformed(sales_raw: pd.DataFrame) -> pd.DataFrame:
    row = sales_raw.iloc[0].to_dict()
    net_revenue = step2_transform(row)
    return pd.DataFrame(
        [
            {
                "sales_total": row["sales_total"],
                "items_sold": row["items_sold"],
                "net_revenue": net_revenue,
            }
        ]
    )
