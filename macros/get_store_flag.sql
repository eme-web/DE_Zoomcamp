 {#
    This macro returns the description of the store_and_fwd_flag 
#}

{% macro get_store_flag(store_and_fwd_flag) -%}

    case {{ store_and_fwd_flag }}
        when 'N' then false
        when 'Y' then true  
    end

{%- endmacro %}