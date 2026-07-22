{% macro filter_ingestion_date(column='ingestion_date', cte='source', start_date=none, end_date=none) %}
{#
    Convention: every staging model's first CTE is named `source` (the raw
    read off `{{ source(...) }}`). This macro filters that CTE by
    ingestion_date without needing to know the underlying table/relation.

    Default (no args, no --vars): latest partition only — correct for
    full-refresh snapshot sources (CRM, ecommerce, manage all replace/
    materialize the whole table on every run, so "latest" == "current state").

    Backfill / reprocess a range:
      dbt build --select stg_ecommerce__orders --vars '{"start_date": "2026-07-01", "end_date": "2026-07-20"}'

    Append-only time-series sources (events/*) should NOT call this macro —
    every partition is a real historical record, not a snapshot to replace.
#}
{%- set start_date = start_date or var('start_date', none) -%}
{%- set end_date = end_date or var('end_date', none) -%}
{%- if start_date and end_date -%}
{{ column }} between '{{ start_date }}' and '{{ end_date }}'
{%- elif start_date -%}
{{ column }} >= '{{ start_date }}'
{%- else -%}
{{ column }} = (select max({{ column }}) from {{ cte }})
{%- endif -%}
{% endmacro %}
