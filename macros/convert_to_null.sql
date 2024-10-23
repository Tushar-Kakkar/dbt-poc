{% macro convert_to_null(col_nm,val)%}
    case when {{col_nm}} = '{{val}}' then null else {{col_nm}} end
{% endmacro %}