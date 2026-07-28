{% macro filter_ingestion_date(column='ingestion_date') %}
{#
    Filters a staging model to the latest ingestion partition by default.
    Every staging models raw CTE must be named `source` (project convention).

    Usage:
      where {{ filter_ingestion_date() }}
      where {{ filter_ingestion_date('ingestion_date') }}   -- same thing, explicit

    Behavior:
      No --vars           → column = (select max(column) from source)
                            Latest partition only. Correct for snapshot sources
                            (CRM, ecommerce, manage replace the whole table on
                            every run, so "latest" == "current state").

      --vars '{"start_date": "2026-07-01"}'
                          → column >= '2026-07-01'

      --vars '{"start_date": "2026-07-01", "end_date": "2026-07-20"}'
                          → column between '2026-07-01' and '2026-07-20'
                            Use for backfills / reprocessing a date range.

    Do NOT call this macro in events/* staging models — those sources are
    append-only (every partition is a unique historical batch, not a snapshot).
#}
{%- set start = var('start_date', none) -%}
{%- set end   = var('end_date',   none) -%}
{%- if start and end -%}
    {{ column }} between '{{ start }}' and '{{ end }}'
{%- elif start -%}
    {{ column }} >= '{{ start }}'
{%- else -%}
    {{ column }} = (select max({{ column }}) from source)
{%- endif -%}
{% endmacro %}
