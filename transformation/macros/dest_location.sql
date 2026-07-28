{% macro dest_location(domain, model, layer='silver') -%}
{%- set path = ('local/' if target.name == 'dev' else 'az://') ~ layer ~ '/' ~ domain ~ '/' ~ model ~ '/' -%}
{{- path -}}
{%- endmacro %}
