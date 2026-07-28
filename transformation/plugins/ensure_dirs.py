import os

from dbt.adapters.duckdb.plugins import BasePlugin


class Plugin(BasePlugin):
    """
    Creates local directories before any model runs.

    DuckDB's `COPY ... TO` (the `external` materialization's write path) never
    creates missing parent directories itself -- it only auto-creates a single
    missing directory level when PARTITION_BY is used, and errors out if more
    than one level is missing. Since `local/silver/` and per-domain folders
    don't exist on a fresh checkout (transformation/local/ is gitignored),
    every new domain needs its base folder created once so that the
    per-model/per-partition folders underneath it can keep being created
    automatically on every run.

    Configured in profiles.yml as a plugin on the `dev` target:
        plugins:
          - module: plugins.ensure_dirs
            config:
              dirs:
                - local/silver/crm
                - local/silver/ecommerce
                - local/silver/events
                - local/silver/manage
    """

    def initialize(self, plugin_config):
        self._dirs = plugin_config.get("dirs", [])

    def configure_connection(self, conn):
        for d in self._dirs:
            os.makedirs(d, exist_ok=True)
