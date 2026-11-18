{{
    config(
        indexes=[
            {'columns': ['order_item_id'], 'unique': true},
            {'columns': ['date_key']},
            {'columns': ['store_key']},
            {'columns': ['product_key']},
            {'columns': ['customer_key']}
        ]
    )
}}

with order_lines as (

    select
        order_item_id,
        order_id,
        customer_id,
        is_walk_in,
        store_id,
        order_ts,
        order_date,
        channel,
        payment_method,
        order_status,
        product_id,
        is_known_product,
        quantity,
        unit_price,
        gross_amount,
        discount_amount,
        net_amount,
        cost_amount
    from {{ ref('int_order_lines') }}

)

select
    order_item_id,
    order_id,
    {{ date_key('order_date') }} as date_key,
    store_id as store_key,
    case when is_known_product then product_id else -1 end as product_key,
    coalesce(customer_id, -1) as customer_key,
    product_id as source_product_id,
    order_ts,
    channel,
    payment_method,
    order_status,
    order_status = 'completed' as is_revenue,
    is_walk_in,
    quantity,
    unit_price,
    gross_amount,
    discount_amount,
    net_amount,
    cost_amount,
    (net_amount - cost_amount)::numeric(12, 2) as gross_margin
from order_lines
