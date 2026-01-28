with source as (
    select * from {{ source('supply_chain', 'RAW_PRODUCTS') }}
),

renamed as (
    select
        ID as product_id,
        NAME as product_name,
        SUPPLIER_ID as supplier_id,
        COST as product_cost,
        PRICE as product_price,
        IS_ACTIVE as is_active
    from source
)

select * from renamed