with order_items as (

    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        discount_amount
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
        order_status
    from {{ ref('stg_orders') }}

),

products as (

    select
        product_id,
        unit_cost
    from {{ ref('stg_products') }}

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
        products.product_id is not null as is_known_product,
        order_items.quantity,
        order_items.unit_price,
        products.unit_cost,
        order_items.discount_amount
    from order_items
    inner join orders
        on order_items.order_id = orders.order_id
    left join products
        on order_items.product_id = products.product_id

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
    quantity,
    unit_price,
    unit_cost,
    (quantity * unit_price)::numeric(12, 2) as gross_amount,
    discount_amount,
    (quantity * unit_price - discount_amount)::numeric(12, 2) as net_amount,
    -- Cost uses the catalogue's current unit_cost (no cost history in the source).
    -- Orphan products have no cost, so cost_amount stays null instead of a fake 0.
    (quantity * unit_cost)::numeric(12, 2) as cost_amount
from joined
