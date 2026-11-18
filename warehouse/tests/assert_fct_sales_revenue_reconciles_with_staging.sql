-- Revenue in fct_sales must equal revenue recomputed directly from the
-- deduplicated staging models, to the cent. Returns a row only on a mismatch.

with staging_revenue as (

    select
        coalesce(sum(order_items.quantity * order_items.unit_price - order_items.discount_amount), 0) as revenue,
        count(*) as line_count
    from {{ ref('stg_order_items') }} as order_items
    inner join {{ ref('stg_orders') }} as orders
        on order_items.order_id = orders.order_id
    where orders.order_status = 'completed'

),

fact_revenue as (

    select
        coalesce(sum(net_amount), 0) as revenue,
        count(*) as line_count
    from {{ ref('fct_sales') }}
    where is_revenue

)

select
    staging_revenue.revenue as staging_revenue,
    fact_revenue.revenue as fact_revenue,
    staging_revenue.line_count as staging_line_count,
    fact_revenue.line_count as fact_line_count
from staging_revenue
cross join fact_revenue
where staging_revenue.revenue <> fact_revenue.revenue
    or staging_revenue.line_count <> fact_revenue.line_count
