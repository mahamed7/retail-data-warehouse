with order_items as (

    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        discount_amount,
        _loaded_at
    from {{ ref('stg_order_items') }}

),

orders as (

    select
        order_id,
        customer_id,
        is_walk_in,
        store_id,
        order_ts,
        order_date,
        channel,
        payment_method,
        order_status,
        _loaded_at
    from {{ ref('stg_orders') }}

),

-- Point-in-time: each line is costed with the product version valid on the order date.
product_versions as (

    select
        product_version_key,
        product_id,
        unit_cost,
        valid_from,
        valid_to
    from {{ ref('int_product_versions') }}

),

joined as (

    select
        order_items.order_item_id,
        order_items.order_id,
        orders.customer_id,
        orders.is_walk_in,
        orders.store_id,
        orders.order_ts,
        orders.order_date,
        orders.channel,
        orders.payment_method,
        orders.order_status,
        order_items.product_id,
        product_versions.product_id is not null as is_known_product,
        product_versions.product_version_key,
        order_items.quantity,
        order_items.unit_price,
        product_versions.unit_cost,
        order_items.discount_amount,
        -- Watermark for incremental models: the later of the order's and the line's load.
        greatest(orders._loaded_at, order_items._loaded_at) as source_loaded_at
    from order_items
    inner join orders
        on order_items.order_id = orders.order_id
    left join product_versions
        on order_items.product_id = product_versions.product_id
        and orders.order_date >= product_versions.valid_from
        and (product_versions.valid_to is null or orders.order_date < product_versions.valid_to)

)

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
    product_version_key,
    quantity,
    unit_price,
    unit_cost,
    (quantity * unit_price)::numeric(12, 2) as gross_amount,
    discount_amount,
    (quantity * unit_price - discount_amount)::numeric(12, 2) as net_amount,
    -- Cost uses the unit_cost in force on the order date (see int_product_versions).
    -- Orphan products have no cost, so cost_amount stays null instead of a fake 0.
    (quantity * unit_cost)::numeric(12, 2) as cost_amount,
    source_loaded_at
from joined
