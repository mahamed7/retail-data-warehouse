with stores as (

    select
        store_id,
        store_name,
        city,
        region,
        store_type,
        opened_date
    from {{ ref('stg_stores') }}

)

select
    -- Source ids are stable integers and the dimension is type 1, so the key is the id.
    store_id as store_key,
    store_id,
    store_name,
    city,
    region,
    store_type,
    opened_date
from stores
