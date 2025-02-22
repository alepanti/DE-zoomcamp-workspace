{#
    Return description of payment_type
#}

{% macro get_payment_type_desc(payment_type) %}
    case cast( {{ payment_type }} as integer)
        when 1 then 'Credit Card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
        else 'EMPTY'
    end
{%- endmacro %}