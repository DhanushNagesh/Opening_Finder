{#- Lower bound of the Wilson score interval for a proportion p over n trials (D-007).
    Draws enter as 0.5, so p is a score rather than a true binomial proportion.
    Clamped because at p = 0 the subtraction rounds to about -2e-17 in doubles. -#}
{% macro wilson_lower_bound(p, n, z=1.96) -%}
    greatest(0.0, (
        ({{ p }}) + {{ z }} * {{ z }} / (2.0 * ({{ n }}))
        - {{ z }} * sqrt(
            ({{ p }}) * (1 - ({{ p }})) / ({{ n }})
            + {{ z }} * {{ z }} / (4.0 * ({{ n }}) * ({{ n }}))
        )
    ) / (1 + {{ z }} * {{ z }} / ({{ n }})))
{%- endmacro %}
