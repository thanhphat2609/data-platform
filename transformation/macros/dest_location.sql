{% macro dest_location(domain, model, layer='silver') %}
{#
    Returns the external write path for an intermediate or mart model.

    Mirrors source_location() on the write side — same target switch,
    same path convention.

    dev  → local/{layer}/{domain}/{model}    (local Parquet file)
    prod → az://{layer}/{domain}/{model}     (Azure Blob Parquet)

    Usage in model config:
      {{ config(
          materialized = 'external',
          location     = dest_location('ecommerce', 'int_orders'),
          format       = 'parquet'
      ) }}

    Layer defaults to 'silver' (intermediate models).
    Pass layer='gold' for mart models when they exist.
#}
{%- if target.name == 'dev' -%}local/{{ layer }}/{{ domain }}/{{ model }}{%- else -%}az://{{ layer }}/{{ domain }}/{{ model }}{%- endif -%}
{% endmacro %}
