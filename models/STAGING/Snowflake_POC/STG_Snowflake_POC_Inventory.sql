with 

source as (

    select * from {{ source('Snowflake_POC', 'inventory') }}

),

renamed as (

    select
        inv_date_sk,
        inv_item_sk,
        inv_warehouse_sk,
        inv_warehouse_sk+10 as inv_warehouse_sk_new,
        inv_quantity_on_hand,
    {{ dbt_utils.generate_surrogate_key(['INV_DATE_SK', 'INV_ITEM_SK', 'INV_WAREHOUSE_SK']) }} as REC_ID

    from source,

)

select * from renamed

