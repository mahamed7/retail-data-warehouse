-- Point-in-time correctness: every sale of a known product must reference the product
-- version whose validity interval contains the order date, and its cost must use that
-- version's unit cost. Returns one row per mis-costed line.

select
    fct_sales.order_item_id,
    fct_sales.order_ts::date as order_date,
    history.product_version_key,
    history.valid_from,
    history.valid_to,
    fct_sales.cost_amount,
    (fct_sales.quantity * history.unit_cost)::numeric(12, 2) as expected_cost_amount
from {{ ref('fct_sales') }} as fct_sales
inner join {{ ref('dim_product_history') }} as history
    on fct_sales.product_version_key = history.product_version_key
where not history.is_unknown_member
    and (
        fct_sales.order_ts::date < history.valid_from
        or fct_sales.order_ts::date >= coalesce(history.valid_to, date '9999-12-31')
        or fct_sales.cost_amount <> (fct_sales.quantity * history.unit_cost)::numeric(12, 2)
    )
