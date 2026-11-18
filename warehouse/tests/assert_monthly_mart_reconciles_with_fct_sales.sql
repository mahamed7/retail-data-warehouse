-- The monthly store mart must add up to the fact table: same completed revenue
-- and same number of orders. Returns a row only on a mismatch.

with mart_totals as (

    select
        coalesce(sum(revenue), 0) as revenue,
        coalesce(sum(total_orders), 0) as total_orders
    from {{ ref('mart_monthly_store_performance') }}

),

fact_totals as (

    select
        coalesce(sum(net_amount) filter (where is_revenue), 0) as revenue,
        count(distinct order_id) as total_orders
    from {{ ref('fct_sales') }}

)

select
    mart_totals.revenue as mart_revenue,
    fact_totals.revenue as fact_revenue,
    mart_totals.total_orders as mart_total_orders,
    fact_totals.total_orders as fact_total_orders
from mart_totals
cross join fact_totals
where mart_totals.revenue <> fact_totals.revenue
    or mart_totals.total_orders <> fact_totals.total_orders
