with source as (

    select * from {{ source('raw', 'order_items') }}

),

renamed as (

    select
        {{ clean_text('order_item_id') }}::integer as order_item_id,
        {{ clean_text('order_id') }}::integer as order_id,
        {{ clean_text('product_id') }}::integer as product_id,
        {{ clean_text('quantity') }}::integer as quantity,
        {{ clean_text('unit_price') }}::numeric(12, 2) as unit_price,
        -- An empty discount means no discount.
        coalesce({{ clean_text('discount_amount') }}::numeric(12, 2), 0)::numeric(12, 2) as discount_amount
    from source

)

select * from renamed
