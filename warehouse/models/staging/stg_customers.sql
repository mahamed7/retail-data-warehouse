with source as (

    select * from {{ source('raw', 'customers') }}

),

renamed as (

    select
        {{ clean_text('customer_id') }}::integer as customer_id,
        initcap({{ clean_text('first_name') }}) as first_name,
        initcap({{ clean_text('last_name') }}) as last_name,
        lower({{ clean_text('email') }}) as email,
        {{ clean_text('phone') }} as phone,
        {{ clean_text('city') }} as city,
        {{ clean_text('signup_date') }}::date as signup_date
    from source

),

-- About 2% of raw rows are exact duplicates. Deduplicating after cleaning also
-- collapses copies that differ only in casing or whitespace. Metadata columns are
-- left out so they cannot keep two copies of the same customer apart.
deduplicated as (

    select distinct
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        city,
        signup_date
    from renamed

)

select
    customer_id,
    first_name,
    last_name,
    concat_ws(' ', first_name, last_name) as full_name,
    email,
    phone,
    city,
    signup_date
from deduplicated
