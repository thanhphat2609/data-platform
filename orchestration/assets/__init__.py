from dagster import load_assets_from_modules

from . import sales_assets, sap_assets

sales = load_assets_from_modules([sales_assets])
sap = load_assets_from_modules([sap_assets])
