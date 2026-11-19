with source as (

    select * from {{ source('raw', 'stores') }}

),

renamed as (

    select
        {{ clean_text('store_id') }}::integer as store_id,
        {{ clean_text('store_name') }} as store_name,
        {{ clean_text('city') }} as city,
        {{ clean_text('region') }} as region,
        {{ clean_text('store_type') }} as store_type,
        {{ clean_text('opened_date') }}::date as opened_date,
        _run_date,
        _loaded_at
    from source

),

-- Each daily batch carries a full snapshot of the stores; the latest one wins.
-- A full-refresh load has a null _run_date, so it only wins when no batch exists.
ranked as (

    select
        store_id,
        store_name,
        city,
        region,
        store_type,
        opened_date,
        row_number() over (
            partition by store_id
            order by _run_date desc nulls last, _loaded_at desc
        ) as snapshot_rank
    from renamed

)

select
    store_id,
    store_name,
    city,
    region,
    store_type,
    opened_date
from ranked
where snapshot_rank = 1
