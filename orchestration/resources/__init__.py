from dagster import EnvVar

from .blob_io_manager import AzureBlobParquetIOManager

blob_io_manager = AzureBlobParquetIOManager(
    connection_str=EnvVar("AZURE_STORAGE_CONNECTION_STRING"),
    container_name=EnvVar("AZURE_STORAGE_CONTAINER"),
    base_folder="data",
)
