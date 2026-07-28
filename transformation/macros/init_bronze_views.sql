{% macro init_bronze_views() %}
{#
    Creates schema-qualified VIEWs over bronze sources before any model runs.
    Called from on-run-start in dbt_project.yml.

    Why here and not pre_init_sql in profiles?
      pre_init_sql runs on a separate init cursor; its DDL is not visible to
      the thread connections that dbt opens for model/test execution.
      on-run-start fires in the primary connection before threads spin up,
      and with a file-based DuckDB all threads share the same file state.

    Dev  → VIEWs over local/ CSV files
    Prod → VIEWs over Azure Blob (Parquet for dlt, Delta for SDP)

    {% if execute %} prevents this from running during dbt parse/compile
    when there is no live database connection.
#}
{% if execute %}

  {% if target.name == 'dev' %}

    {# ── CRM — local CSV ──────────────────────────────────────────────────── #}
    {% do run_query("CREATE SCHEMA IF NOT EXISTS crm") %}
    {% do run_query("CREATE OR REPLACE VIEW crm.customers AS SELECT * FROM read_csv('local/crm/customers.csv')") %}
    {% do run_query("CREATE OR REPLACE VIEW crm.orders    AS SELECT * FROM read_csv('local/crm/orders.csv')") %}
    {% do run_query("CREATE OR REPLACE VIEW crm.products  AS SELECT * FROM read_csv('local/crm/products.csv')") %}

    {# ── Ecommerce — local CSV ────────────────────────────────────────────── #}
    {% do run_query("CREATE SCHEMA IF NOT EXISTS ecommerce") %}
    {% do run_query("CREATE OR REPLACE VIEW ecommerce.customers   AS SELECT * FROM read_csv('local/ecommerce/bronze_customers.csv')") %}
    {% do run_query("CREATE OR REPLACE VIEW ecommerce.categories  AS SELECT * FROM read_csv('local/ecommerce/bronze_categories.csv')") %}
    {% do run_query("CREATE OR REPLACE VIEW ecommerce.products    AS SELECT * FROM read_csv('local/ecommerce/bronze_products.csv')") %}
    {% do run_query("CREATE OR REPLACE VIEW ecommerce.orders      AS SELECT * FROM read_csv('local/ecommerce/bronze_orders.csv')") %}
    {% do run_query("CREATE OR REPLACE VIEW ecommerce.order_items AS SELECT * FROM read_csv('local/ecommerce/bronze_order_items.csv')") %}

    {# ── Events — local CSV ───────────────────────────────────────────────── #}
    {% do run_query("CREATE SCHEMA IF NOT EXISTS events") %}
    {% do run_query("CREATE OR REPLACE VIEW events.earthquake_events AS SELECT * FROM read_csv('local/events/earthquake_events.csv')") %}
    {% do run_query("CREATE OR REPLACE VIEW events.exchange_rates    AS SELECT * FROM read_csv('local/events/exchange_rates.csv')") %}
    {% do run_query("CREATE OR REPLACE VIEW events.openmeteo_hourly  AS SELECT * FROM read_csv('local/events/openmeteo_hourly.csv')") %}

    {# ── Manage — local CSV ───────────────────────────────────────────────── #}
    {% do run_query("CREATE SCHEMA IF NOT EXISTS manage") %}
    {% do run_query("CREATE OR REPLACE VIEW manage.users    AS SELECT * FROM read_csv('local/manage/bronze_users.csv')") %}
    {% do run_query("CREATE OR REPLACE VIEW manage.projects AS SELECT * FROM read_csv('local/manage/bronze_projects.csv')") %}
    {% do run_query("CREATE OR REPLACE VIEW manage.tasks    AS SELECT * FROM read_csv('local/manage/bronze_tasks.csv')") %}
    {% do run_query("CREATE OR REPLACE VIEW manage.issues   AS SELECT * FROM read_csv('local/manage/bronze_issues.csv')") %}
    {% do run_query("CREATE OR REPLACE VIEW manage.comments AS SELECT * FROM read_csv('local/manage/bronze_comments.csv')") %}

  {% else %}

    {# ── CRM — Azure Blob Parquet (Hive-partitioned by ingestion_date) ───── #}
    {% do run_query("CREATE SCHEMA IF NOT EXISTS crm") %}
    {% do run_query("CREATE OR REPLACE VIEW crm.customers AS SELECT * FROM read_parquet('az://bronze/dlt/crm/customers/*/*.parquet', hive_partitioning=true)") %}
    {% do run_query("CREATE OR REPLACE VIEW crm.orders    AS SELECT * FROM read_parquet('az://bronze/dlt/crm/orders/*/*.parquet',    hive_partitioning=true)") %}
    {% do run_query("CREATE OR REPLACE VIEW crm.products  AS SELECT * FROM read_parquet('az://bronze/dlt/crm/products/*/*.parquet',  hive_partitioning=true)") %}

    {# ── Ecommerce — Azure Blob Delta ─────────────────────────────────────── #}
    {% do run_query("CREATE SCHEMA IF NOT EXISTS ecommerce") %}
    {% do run_query("CREATE OR REPLACE VIEW ecommerce.customers   AS SELECT * FROM delta_scan('az://bronze/sdp/ecommerce/bronze_customers')") %}
    {% do run_query("CREATE OR REPLACE VIEW ecommerce.categories  AS SELECT * FROM delta_scan('az://bronze/sdp/ecommerce/bronze_categories')") %}
    {% do run_query("CREATE OR REPLACE VIEW ecommerce.products    AS SELECT * FROM delta_scan('az://bronze/sdp/ecommerce/bronze_products')") %}
    {% do run_query("CREATE OR REPLACE VIEW ecommerce.orders      AS SELECT * FROM delta_scan('az://bronze/sdp/ecommerce/bronze_orders')") %}
    {% do run_query("CREATE OR REPLACE VIEW ecommerce.order_items AS SELECT * FROM delta_scan('az://bronze/sdp/ecommerce/bronze_order_items')") %}

    {# ── Events — Azure Blob Parquet (Hive-partitioned by ingestion_date) ── #}
    {% do run_query("CREATE SCHEMA IF NOT EXISTS events") %}
    {% do run_query("CREATE OR REPLACE VIEW events.earthquake_events AS SELECT * FROM read_parquet('az://bronze/dlt/events/earthquake_events/*/*.parquet', hive_partitioning=true)") %}
    {% do run_query("CREATE OR REPLACE VIEW events.exchange_rates    AS SELECT * FROM read_parquet('az://bronze/dlt/events/exchange_rates/*/*.parquet',    hive_partitioning=true)") %}
    {% do run_query("CREATE OR REPLACE VIEW events.openmeteo_hourly  AS SELECT * FROM read_parquet('az://bronze/dlt/events/openmeteo_hourly/*/*.parquet',  hive_partitioning=true)") %}

    {# ── Manage — Azure Blob Delta ────────────────────────────────────────── #}
    {% do run_query("CREATE SCHEMA IF NOT EXISTS manage") %}
    {% do run_query("CREATE OR REPLACE VIEW manage.users    AS SELECT * FROM delta_scan('az://bronze/sdp/manage/bronze_users')") %}
    {% do run_query("CREATE OR REPLACE VIEW manage.projects AS SELECT * FROM delta_scan('az://bronze/sdp/manage/bronze_projects')") %}
    {% do run_query("CREATE OR REPLACE VIEW manage.tasks    AS SELECT * FROM delta_scan('az://bronze/sdp/manage/bronze_tasks')") %}
    {% do run_query("CREATE OR REPLACE VIEW manage.issues   AS SELECT * FROM delta_scan('az://bronze/sdp/manage/bronze_issues')") %}
    {% do run_query("CREATE OR REPLACE VIEW manage.comments AS SELECT * FROM delta_scan('az://bronze/sdp/manage/bronze_comments')") %}

  {% endif %}

{% endif %}
select 1 as init_complete
{% endmacro %}
