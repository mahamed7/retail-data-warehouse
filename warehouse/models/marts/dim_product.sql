with products as (

    select
        product_id,
        sku,
        product_name,
        category,
        subcategory,
        unit_cost,
        unit_price,
        is_active
    from {{ ref('stg_products') }}

),

catalogue as (

    select
        -- Source ids are stable integers and the dimension is type 1, so the key is the id.
        product_id as product_key,
        product_id,
        sku,
        product_name,
        category,
        subcategory,
        unit_cost,
        unit_price,
        (unit_price - unit_cost)::numeric(12, 2) as list_margin,
        is_active,
        false as is_unknown_member
    from products

),

-- Order lines that reference a product missing from the catalogue point here.
unknown_member as (

    select
        -1 as product_key,
        null::integer as product_id,
        'UNKNOWN' as sku,
        'Unknown product' as product_name,
        'Unknown' as category,
        'Unknown' as subcategory,
        null::numeric(12, 2) as unit_cost,
        null::numeric(12, 2) as unit_price,
        null::numeric(12, 2) as list_margin,
        false as is_active,
        true as is_unknown_member

)

select
    product_key,
    product_id,
    sku,
    product_name,
    category,
    subcategory,
    unit_cost,
    unit_price,
    list_margin,
    is_active,
    is_unknown_member
from catalogue

union all

select
    product_key,
    product_id,
    sku,
    product_name,
    category,
    subcategory,
    unit_cost,
    unit_price,
    list_margin,
    is_active,
    is_unknown_member
from unknown_member
