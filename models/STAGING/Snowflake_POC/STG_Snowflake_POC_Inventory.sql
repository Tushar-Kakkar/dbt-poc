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
        inv_quantity_on_hand

    from source

)

select * from renamed
