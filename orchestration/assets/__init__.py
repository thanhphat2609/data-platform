from dagster import load_assets_from_modules

from . import dlt_assets, sales_assets, sap_assets, sdp_assets

sales = load_assets_from_modules([sales_assets])
sap   = load_assets_from_modules([sap_assets])
dlt   = load_assets_from_modules([dlt_assets])
sdp   = load_assets_from_modules([sdp_assets])
