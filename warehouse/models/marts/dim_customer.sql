with customers as (

    select
        customer_id,
        first_name,
        last_name,
        full_name,
        email,
        phone,
        city,
        signup_date
    from {{ ref('stg_customers') }}

),

completed_orders as (

    select
        customer_id,
        order_id,
        order_date
    from {{ ref('stg_orders') }}
    where order_status = 'completed'
        and customer_id is not null

),

completed_lines as (

    select
        customer_id,
        net_amount
    from {{ ref('int_order_lines') }}
    where order_status = 'completed'
        and customer_id is not null

),

order_summary as (

    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        count(order_id)::integer as lifetime_orders
    from completed_orders
    group by customer_id

),

revenue_summary as (

    select
        customer_id,
        sum(net_amount)::numeric(14, 2) as lifetime_revenue
    from completed_lines
    group by customer_id

),

registered as (

    select
        -- Source ids are stable integers and the dimension is type 1, so the key is the id.
        customers.customer_id as customer_key,
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customers.full_name,
        customers.email,
        customers.phone,
        customers.city,
        customers.signup_date,
        order_summary.first_order_date,
        order_summary.last_order_date,
        coalesce(order_summary.lifetime_orders, 0) as lifetime_orders,
        coalesce(revenue_summary.lifetime_revenue, 0)::numeric(14, 2) as lifetime_revenue,
        case
            when coalesce(order_summary.lifetime_orders, 0) = 0 then 'No purchases'
            when order_summary.lifetime_orders < {{ var('returning_min_orders') }} then 'New'
            when order_summary.lifetime_orders < {{ var('loyal_min_orders') }} then 'Returning'
            else 'Loyal'
        end as customer_segment,
        false as is_walk_in
    from customers
    left join order_summary
        on customers.customer_id = order_summary.customer_id
    left join revenue_summary
        on customers.customer_id = revenue_summary.customer_id

),

-- Orders without a customer_id are walk-ins; they all share this member.
walk_in_member as (

    select
        -1 as customer_key,
        null::integer as customer_id,
        null::text as first_name,
        null::text as last_name,
        'Walk-in customer' as full_name,
        null::text as email,
        null::text as phone,
        null::text as city,
        null::date as signup_date,
        null::date as first_order_date,
        null::date as last_order_date,
        0 as lifetime_orders,
        0::numeric(14, 2) as lifetime_revenue,
        'Walk-in' as customer_segment,
        true as is_walk_in

)

select
    customer_key,
    customer_id,
    first_name,
    last_name,
    full_name,
    email,
    phone,
    city,
    signup_date,
    first_order_date,
    last_order_date,
    lifetime_orders,
    lifetime_revenue,
    customer_segment,
    is_walk_in
from registered

union all

select
    customer_key,
    customer_id,
    first_name,
    last_name,
    full_name,
    email,
    phone,
    city,
    signup_date,
    first_order_date,
    last_order_date,
    lifetime_orders,
    lifetime_revenue,
    customer_segment,
    is_walk_in
from walk_in_member
