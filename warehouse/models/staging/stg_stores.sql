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
        {{ clean_text('opened_date') }}::date as opened_date
    from source

)

select * from renamed
