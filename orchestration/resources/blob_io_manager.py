import io

import pandas as pd
from dagster import ConfigurableIOManager, InputContext, OutputContext


class AzureBlobParquetIOManager(ConfigurableIOManager):
    """Persists every asset as a Parquet file in Azure Blob Storage.

    Blob path: <base_folder>/<asset_key_parts...>.parquet
    Example:   data/sales/sales_raw.parquet
    """

    connection_str: str
    container_name: str
    base_folder: str = "data"

    def _blob_path(self, asset_key) -> str:
        parts = list(asset_key.path)
        path = "/".join(parts) + ".parquet"
        return f"{self.base_folder}/{path}" if self.base_folder else path

    def handle_output(self, context: OutputContext, obj: pd.DataFrame) -> None:
        from azure.storage.blob import BlobServiceClient

        if not isinstance(obj, pd.DataFrame):
            raise TypeError(f"AzureBlobParquetIOManager expects pd.DataFrame, got {type(obj)}")

        blob_path = self._blob_path(context.asset_key)
        container = BlobServiceClient.from_connection_string(self.connection_str).get_container_client(
            self.container_name
        )

        buf = io.BytesIO()
        obj.to_parquet(buf, index=False)
        buf.seek(0)
        container.upload_blob(blob_path, buf, overwrite=True)

        context.log.info(f"Wrote {len(obj)} rows → {self.container_name}/{blob_path}")
        context.add_output_metadata(
            {
                "blob_path": blob_path,
                "row_count": len(obj),
                "columns": list(obj.columns),
            }
        )

    def load_input(self, context: InputContext) -> pd.DataFrame:
        from azure.storage.blob import BlobServiceClient

        blob_path = self._blob_path(context.asset_key)
        container = BlobServiceClient.from_connection_string(self.connection_str).get_container_client(
            self.container_name
        )

        data = container.get_blob_client(blob_path).download_blob().readall()
        df = pd.read_parquet(io.BytesIO(data))

        context.log.info(f"Read {len(df)} rows ← {self.container_name}/{blob_path}")
        return df
