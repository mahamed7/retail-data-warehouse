-- Every order must fall inside the sales period from the data contract.
-- Returns the offending orders.

select
    order_id,
    order_ts
from {{ ref('stg_orders') }}
where order_ts < '{{ var("sales_start_date") }}'::date
    or order_ts >= '{{ var("sales_end_date") }}'::date + 1
