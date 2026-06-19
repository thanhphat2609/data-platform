# Ingestion

EL layer — three tools covering every source type and scale.

## Tools at a glance

| Tool | Best for | Output format |
|------|----------|---------------|
| [dlt](dlt/README.md) | PostgreSQL CDC · custom REST APIs | Parquet |
| [Spark SDP](sdp/README.md) | Large-scale JDBC batch (MySQL, PostgreSQL) | Delta Lake |
| [Airbyte](airbyte/README.md) | SaaS APIs with existing connectors (zero code) | Parquet |

## Architecture

```
PostgreSQL (CDC)   MySQL (JDBC)   PostgreSQL (JDBC)   GitHub / Open-Meteo / FX
      │                 │                │                       │
      ▼                 ▼                ▼                       ▼
   [ dlt ]        [ Spark SDP ]   [ Spark SDP ]          [ Airbyte CE ]
      │                 │                │                       │
      └─────────────────┴────────────────┴───────────────────────┘
                                   │
                      ┌────────────▼────────────┐
                      │    MinIO  (bronze/)      │
                      │   Parquet / Delta Lake   │
                      └─────────────────────────┘
```

## Tool selection guide

| Source | Scale | Pick |
|--------|-------|------|
| SaaS API (GitHub, Stripe, Salesforce…) | Any | **Airbyte** |
| PostgreSQL row-level CDC | Medium | **dlt** |
| Custom REST with pagination / auth | Medium | **dlt** |
| MySQL or PostgreSQL full-table dump | Large (1M+ rows) | **Spark SDP** |

→ Full decision matrix: [docs/when-to-use.md](docs/when-to-use.md)

## Bronze output paths

| Source | Path | Format |
|--------|------|--------|
| dlt PostgreSQL CDC | `bronze/dlt/postgres/<table>/_dlt_load_id=*/` | Parquet |
| dlt REST APIs | `bronze/dlt/api/<stream>/_dlt_load_id=*/` | Parquet |
| Spark MySQL/ecommerce | `bronze/sdp/mysql/sales/bronze_<table>/` | Delta |
| Spark PostgreSQL/manage | `bronze/sdp/postgresql/manage/bronze_<table>/` | Delta |
| Airbyte | `bronze/airbyte/<source>/<stream>/YYYY/MM/DD/` | Parquet |

→ Naming rules and partition strategy: [docs/bronze-convention.md](docs/bronze-convention.md)

## Docs

| Doc | Purpose |
|-----|---------|
| [docs/architecture.md](docs/architecture.md) | Full architecture diagram and component breakdown |
| [docs/when-to-use.md](docs/when-to-use.md) | Tool decision matrix with anti-patterns |
| [docs/bronze-convention.md](docs/bronze-convention.md) | Folder structure and partition strategy for bronze layer |
