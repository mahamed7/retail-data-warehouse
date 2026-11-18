with source as (

    select * from {{ source('raw', 'products') }}

),

renamed as (

    select
        {{ clean_text('product_id') }}::integer as product_id,
        upper({{ clean_text('sku') }}) as sku,
        {{ clean_text('product_name') }} as product_name,
        {{ clean_text('category') }} as category,
        {{ clean_text('subcategory') }} as subcategory,
        {{ clean_text('unit_cost') }}::numeric(12, 2) as unit_cost,
        {{ clean_text('unit_price') }}::numeric(12, 2) as unit_price,
        lower({{ clean_text('is_active') }})::boolean as is_active
    from source

)

select * from renamed
