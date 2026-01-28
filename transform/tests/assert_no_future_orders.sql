-- This test passes if it returns 0 rows.
-- If it returns rows, those are the "errors" and the test fails.

select 
    order_id, 
    order_date
from {{ ref('stg_orders') }}
where order_date > current_date()