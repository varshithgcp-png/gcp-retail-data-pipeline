{% macro safe_rate(numerator, denominator) %}
safe_divide(cast({{ numerator }} as float64), nullif(cast({{ denominator }} as float64), 0))
{% endmacro %}
