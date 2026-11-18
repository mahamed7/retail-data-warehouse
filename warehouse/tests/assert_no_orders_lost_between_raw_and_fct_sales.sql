-- No silent loss: every order id in raw.orders must reach fct_sales with all of
-- its raw lines, unless it has no lines at all. Reads the raw source directly so
-- a bug in staging deduplication or a join cannot hide itself.
-- Returns each order whose line count differs between raw and the fact table.

with raw_orders as (

    select distinct
        {{ clean_text('order_id') }}::integer as order_id
    from {{ source('raw', 'orders') }}

),

raw_items as (

    select
        {{ clean_text('order_id') }}::integer as order_id
    from {{ source('raw', 'order_items') }}

),

raw_lines as (

    select
        order_id,
        count(*) as line_count
    from raw_items
    group by order_id

),

fact_lines as (

    select
        order_id,
        count(*) as line_count
    from {{ ref('fct_sales') }}
    group by order_id

)

select
    raw_orders.order_id,
    coalesce(raw_lines.line_count, 0) as raw_line_count,
    coalesce(fact_lines.line_count, 0) as fact_line_count
from raw_orders
left join raw_lines
    on raw_orders.order_id = raw_lines.order_id
left join fact_lines
    on raw_orders.order_id = fact_lines.order_id
where coalesce(raw_lines.line_count, 0) <> coalesce(fact_lines.line_count, 0)
