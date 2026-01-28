with order_details as (
    select * from {{ ref('int_order_details') }}
),

suppliers as (
    select * from {{ ref('stg_suppliers') }}
),

joined_data as (
    select
        order_details.*,
        suppliers.supplier_name,
        suppliers.country
    from order_details
    left join suppliers on order_details.supplier_id = suppliers.supplier_id
)

select
    supplier_id,
    supplier_name,
    country,
    date_trunc('month', order_date) as order_month,
    
    count(distinct order_id) as total_orders,
    sum(quantity) as total_items_sold,
    sum(gross_revenue) as total_revenue,
    sum(net_profit) as total_profit

from joined_data
group by 1, 2, 3, 4