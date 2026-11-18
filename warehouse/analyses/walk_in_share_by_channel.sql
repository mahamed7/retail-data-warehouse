-- How much of each channel's business comes from walk-in versus registered customers?
-- Completed orders only.

with channel_orders as (

    select
        channel,
        is_walk_in,
        count(distinct order_id) as orders,
        sum(net_amount) as revenue
    from {{ ref('fct_sales') }}
    where is_revenue
    group by
        channel,
        is_walk_in

)

select
    channel,
    case when is_walk_in then 'Walk-in' else 'Registered' end as customer_type,
    orders,
    revenue,
    round(orders::numeric / sum(orders) over (partition by channel), 4) as order_share,
    round(revenue / sum(revenue) over (partition by channel), 4) as revenue_share
from channel_orders
order by
    channel,
    customer_type
