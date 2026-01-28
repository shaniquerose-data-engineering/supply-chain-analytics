with source as (
    select * from {{ source('supply_chain', 'RAW_ORDERS') }}
),

renamed as (
    select
        ID as order_id,
        PRODUCT_ID as product_id,
        QUANTITY as quantity,
        ORDER_DATE as order_date,
        STATUS as status
    from source
)

select * from renamed