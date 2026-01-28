with orders as (
    select * from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

joined as (
    select
        orders.order_id,
        orders.order_date,
        orders.status,
        
        -- Join Product Details
        products.product_id,
        products.supplier_id,  -- <--- ADDED THIS LINE
        products.product_name,
        products.product_cost,
        products.product_price,
        
        -- Metrics Calculation
        orders.quantity,
        (orders.quantity * products.product_price) as gross_revenue,
        (orders.quantity * (products.product_price - products.product_cost)) as net_profit

    from orders
    left join products on orders.product_id = products.product_id
)

select * from joined