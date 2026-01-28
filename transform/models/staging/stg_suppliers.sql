with source as (
    select * from {{ source('supply_chain', 'RAW_SUPPLIERS') }}
),

renamed as (
    select
        ID as supplier_id,
        NAME as supplier_name,
        -- Normalize Country Names
        case 
            when COUNTRY in ('USA', 'US', 'United States') then 'United States'
            else COUNTRY
        end as country,
        CREATED_AT as onboard_date
    from source
)

select * from renamed