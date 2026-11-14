{% macro fit_return_flag(return_reason, fit_feedback) %}
case
  when upper({{ return_reason }}) in ('SIZE_TOO_SMALL', 'SIZE_TOO_LARGE', 'FIT_ISSUE')
    or upper(coalesce({{ fit_feedback }}, '')) in ('RUNS_SMALL', 'RUNS_LARGE')
  then 1
  else 0
end
{% endmacro %}
