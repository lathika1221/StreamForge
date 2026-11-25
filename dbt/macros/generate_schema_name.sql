{#- Use the configured schema verbatim (STAGING / ANALYTICS / OPS), not dbt's default
    "<target_schema>_<custom_schema>" naming. These schemas already exist (M4). -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim | upper }}
    {%- endif -%}
{%- endmacro %}
