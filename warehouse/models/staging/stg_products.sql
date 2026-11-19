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
        lower({{ clean_text('is_active') }})::boolean as is_active,
        _run_date,
        _loaded_at
    from source

),

-- Each daily batch carries a full snapshot of the catalogue; the latest one wins.
-- A full-refresh load has a null _run_date, so it only wins when no batch exists.
ranked as (

    select
        product_id,
        sku,
        product_name,
        category,
        subcategory,
        unit_cost,
        unit_price,
        is_active,
        row_number() over (
            partition by product_id
            order by _run_date desc nulls last, _loaded_at desc
        ) as snapshot_rank
    from renamed

)

select
    product_id,
    sku,
    product_name,
    category,
    subcategory,
    unit_cost,
    unit_price,
    is_active
from ranked
where snapshot_rank = 1
