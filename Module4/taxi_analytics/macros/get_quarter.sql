{#
    Return quarter of pickup_datetime
#}

{% macro get_quarter(pickup_datetime) %}
    case 
        when EXTRACT(MONTH FROM {{ pickup_datetime }}) < 4 then 'Q1'
        when EXTRACT(MONTH FROM {{ pickup_datetime }}) < 7 then 'Q2'
        when EXTRACT(MONTH FROM {{ pickup_datetime }}) < 10 then 'Q3'
        else 'Q4'
    end
{%- endmacro %}