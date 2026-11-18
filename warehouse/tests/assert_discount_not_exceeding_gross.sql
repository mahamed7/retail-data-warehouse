-- A line discount can never be larger than the line's gross amount, so net
-- revenue is never negative. Returns the offending lines.

select
    order_item_id,
    gross_amount,
    discount_amount
from {{ ref('fct_sales') }}
where discount_amount > gross_amount
