with source as (

    select * from {{ source('raw', 'orders') }}

),

renamed as (

    select
        {{ clean_text('order_id') }}::integer as order_id,
        {{ clean_text('customer_id') }}::integer as customer_id,
        {{ clean_text('store_id') }}::integer as store_id,
        {{ clean_text('order_ts') }}::timestamp as order_ts,
        lower({{ clean_text('channel') }}) as channel,
        lower({{ clean_text('payment_method') }}) as payment_method,
        lower({{ clean_text('status') }}) as order_status,
        {{ clean_text('updated_at') }}::timestamp as updated_at
    from source

),

-- About 1% of orders were re-sent after a status change; the latest version wins.
ranked as (

    select
        order_id,
        customer_id,
        store_id,
        order_ts,
        channel,
        payment_method,
        order_status,
        updated_at,
        row_number() over (
            partition by order_id
            order by updated_at desc nulls last
        ) as version_rank
    from renamed

)

select
    order_id,
    customer_id,
    customer_id is null as is_walk_in,
    store_id,
    order_ts,
    order_ts::date as order_date,
    channel,
    payment_method,
    order_status,
    updated_at
from ranked
where version_rank = 1
