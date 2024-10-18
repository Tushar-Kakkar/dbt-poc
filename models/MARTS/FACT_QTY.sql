SELECT
    w.w_warehouse_name,
    i.i_category,
    iv.INV_WAREHOUSE_SK_NEW,
    SUM(iv.inv_quantity_on_hand) AS total_stock
FROM {{ ref('STG_Snowflake_POC_Inventory') }} iv
JOIN {{ source('Snowflake_POC', 'item') }} i ON iv.inv_item_sk = i.i_item_sk
JOIN {{ source('Snowflake_POC', 'warehouse') }} w ON iv.inv_warehouse_sk = w.w_warehouse_sk
GROUP BY w.w_warehouse_name, i.i_category ,iv.INV_WAREHOUSE_SK_NEW
