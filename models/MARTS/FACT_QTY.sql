
SELECT
    w.w_warehouse_name,
    i.i_category,
    iv.INV_WAREHOUSE_SK_NEW,
    SUM(iv.inv_quantity_on_hand) AS total_stock
FROM {{ ref('STG_Snowflake_POC_Inventory') }} iv
JOIN {{ ref('STG_Snowflake_POC_Item') }} i ON iv.inv_item_sk = i.i_item_sk
JOIN {{ ref('STG_Snowflake_POC_Warehouse') }} w ON iv.inv_warehouse_sk = w.w_warehouse_sk
GROUP BY w.w_warehouse_name, i.i_category ,iv.INV_WAREHOUSE_SK_NEW
