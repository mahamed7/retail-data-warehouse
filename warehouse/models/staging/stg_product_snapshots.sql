-- Every catalogue snapshot the source has sent: one row per product per snapshot day.
-- Daily batches each carry a full snapshot, so raw.products keeps the price history
-- that int_product_versions turns into type 2 versions. A full-refresh load has no
-- run date and contributes a single snapshot dated by its load.

with source as (

    select * from {{ source('raw', 'products') }}

),

renamed as (

    select
        {{ clean_text('product_id') }}::integer as product_id,
        {{ clean_text('unit_cost') }}::numeric(12, 2) as unit_cost,
        {{ clean_text('unit_price') }}::numeric(12, 2) as unit_price,
        lower({{ clean_text('is_active') }})::boolean as is_active,
        coalesce(_run_date, _loaded_at::date) as snapshot_date,
        _loaded_at
    from source

),

-- A replayed day is reloaded in place, but stay defensive: one row per product per day.
ranked as (

    select
        product_id,
        snapshot_date,
        unit_cost,
        unit_price,
        is_active,
        row_number() over (
            partition by product_id, snapshot_date
            order by _loaded_at desc
        ) as snapshot_rank
    from renamed

)

select
    product_id,
    snapshot_date,
    unit_cost,
    unit_price,
    is_active
from ranked
where snapshot_rank = 1
