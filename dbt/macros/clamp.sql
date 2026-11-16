{#- Clamp x to [-bound, bound], keeping NULL as NULL. DuckDB's greatest()/least() skip
    NULL arguments, so least(greatest(NULL, -1000), 1000) is -1000, not NULL. -#}
{% macro clamp(x, bound) -%}
    case when ({{ x }}) is null then null
         else least(greatest({{ x }}, -{{ bound }}), {{ bound }}) end
{%- endmacro %}
