-- Reformat date from YYYY-MM-DD to DD/MM/YYYY
{% macro reformatting_date(date) %}
    to_char(cast({{date}} as date), 'DD/MM/YYYY')
{% endmacro %}


-- Extract month and year from date
{% macro extract_month(date) %}
    extract (month from (cast({{date}} as date)))
{% endmacro %}

-- Extract year from date
{% macro extract_year(date) %}
    extract (year from (cast({{date}} as date)))
{% endmacro %}