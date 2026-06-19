import pandas as pd
from dagster import asset

from src.sap_processor import step1_extract_from_sap, step2_validate_and_transform

_QUEUE = {"dagster-celery/queue": "worker-1"}


@asset(
    key_prefix=["sap"],
    group_name="sap",
    op_tags=_QUEUE,
    description="Raw SAP purchase order records in USD.",
)
def sap_raw() -> pd.DataFrame:
    records = step1_extract_from_sap()
    return pd.DataFrame(records)


@asset(
    key_prefix=["sap"],
    group_name="sap",
    op_tags=_QUEUE,
    description="Validated SAP records with amounts converted from USD to VND.",
)
def sap_transformed(sap_raw: pd.DataFrame) -> pd.DataFrame:
    records = sap_raw.to_dict("records")
    cleaned = step2_validate_and_transform(records)
    return pd.DataFrame(cleaned)
