{{
    config(
        materialized = 'incremental',
        unique_key = 'i_item_sk',
        on_schema_change = 'sync_all_columns',
        merge_update_columns = ['i_size', 'i_formulation']
    )
}}

with source as (

    select * from {{ source('Snowflake_POC', 'item') }}
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where i_rec_start_date > (select dateadd('day', -3, max(i_rec_start_date)) from {{ this }}) 
    {% endif %}

),

renamed as (

    select
        i_item_sk,
        i_item_id,
        i_rec_start_date,
        i_rec_end_date,
        i_item_desc,
        i_current_price,
        i_wholesale_cost,
        i_brand_id,
        i_brand,
        i_class_id,
        i_class,
        i_category_id,
        i_category,
        i_manufact_id,
        i_manufact,
        i_size,
        i_formulation,
        i_color,
        i_units,
        i_container,
        i_manager_id,
        i_product_name

    from source

)

select * from renamed