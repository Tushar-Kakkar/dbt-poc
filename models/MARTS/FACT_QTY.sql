{% set query_to_run %}
select distinct i_size from {{ ref('STG_Snowflake_POC_Item') }}
{% endset %}

{% set results = run_query(query_to_run) %}

{% if execute %}
   {% set item_sizes= results.columns[0].values() %}
{% endif %}


with 

inv as (

    select * from {{ ref('STG_Snowflake_POC_Inventory') }}

),

itm as (

    select * from {{ ref('STG_Snowflake_POC_Item') }}

),

wh as (

    select * from {{ ref('STG_Snowflake_POC_Warehouse') }}

),

final_cte as (

SELECT
    w.w_warehouse_name,
    i.i_category,
    iv.INV_WAREHOUSE_SK_NEW,
    SUM(iv.inv_quantity_on_hand) AS total_stock,
    {% for size in item_sizes %}
        {% if size == 'N/A' %}
            sum(case when i_size = 'N/A' then iv.inv_quantity_on_hand end) as total_stock_NA,
        {% elif size == 'extra large' %}
            sum(case when i_size = 'extra large' then iv.inv_quantity_on_hand end) as total_stock_extra_large,
        {% else %}
            sum(case when i.i_size = '{{ size }}' then iv.inv_quantity_on_hand end) as total_stock_{{size}},
        {% endif %}
    {% endfor %}
FROM inv iv
JOIN itm i ON iv.inv_item_sk = i.i_item_sk
JOIN wh w ON iv.inv_warehouse_sk = w.w_warehouse_sk
GROUP BY w.w_warehouse_name, i.i_category ,iv.INV_WAREHOUSE_SK_NEW

)

select * from final_cte
