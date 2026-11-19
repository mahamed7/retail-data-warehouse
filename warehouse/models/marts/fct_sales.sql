{{
    config(
        materialized='incremental',
        unique_key='order_item_id',
        incremental_strategy='delete+insert',
        on_schema_change='fail',
        indexes=[
            {'columns': ['order_item_id'], 'unique': true},
            {'columns': ['date_key']},
            {'columns': ['store_key']},
            {'columns': ['product_key']},
            {'columns': ['customer_key']},
            {'columns': ['product_version_key']},
            {'columns': ['source_loaded_at']}
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
        product_version_key,
        quantity,
        unit_price,
        gross_amount,
        discount_amount,
        net_amount,
        cost_amount,
        source_loaded_at
    from {{ ref('int_order_lines') }}

    {% if is_incremental() %}
    -- Re-process every line whose order or line was (re)loaded since the last run, with a
    -- lookback so late re-sent orders still update. delete+insert replaces them by key.
    where source_loaded_at > (
        select
            coalesce(max(source_loaded_at), '-infinity'::timestamptz)
            - interval '{{ var("incremental_lookback_hours") }} hours'
        from {{ this }}
    )
    {% endif %}

)

select
    order_item_id,
    order_id,
    {{ date_key('order_date') }} as date_key,
    store_id as store_key,
    case when is_known_product then product_id else -1 end as product_key,
    coalesce(product_version_key, -1) as product_version_key,
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
    (net_amount - cost_amount)::numeric(12, 2) as gross_margin,
    source_loaded_at
from order_lines
