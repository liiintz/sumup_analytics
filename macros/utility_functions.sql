{%- macro clean_product_sku(column) -%}
    case 
        when ltrim({{ column }}, 'v') like '%.0' then substr(ltrim({{ column }}, 'v'), 1, length(ltrim({{ column }}, 'v')) - 2)
        else ltrim({{ column }}, 'v')
    end
{%- endmacro -%}
