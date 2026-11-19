-- Type 2 product dimension: one row per product version. Price, cost and active flag are
-- tracked historically (from int_product_versions); name, SKU and category are type 1 and
-- always show the latest catalogue values. fct_sales joins here on product_version_key to
-- cost each sale at the price list in force on the day it happened.

with versions as (

    select
        product_version_key,
        product_id,
        version_number,
        unit_cost,
        unit_price,
        is_active,
        valid_from,
        valid_to,
        is_current,
        first_seen_date
    from {{ ref('int_product_versions') }}

),

current_catalogue as (

    select
        product_id,
        sku,
        product_name,
        category,
        subcategory
    from {{ ref('stg_products') }}

),

history as (

    select
        versions.product_version_key,
        versions.product_id as product_key,
        versions.product_id,
        current_catalogue.sku,
        current_catalogue.product_name,
        current_catalogue.category,
        current_catalogue.subcategory,
        versions.version_number,
        versions.unit_cost,
        versions.unit_price,
        (versions.unit_price - versions.unit_cost)::numeric(12, 2) as list_margin,
        versions.is_active,
        versions.valid_from,
        versions.valid_to,
        versions.is_current,
        versions.first_seen_date,
        false as is_unknown_member
    from versions
    inner join current_catalogue
        on versions.product_id = current_catalogue.product_id

),

-- Order lines that reference a product missing from the catalogue point here.
unknown_member as (

    select
        -1 as product_version_key,
        -1 as product_key,
        null::integer as product_id,
        'UNKNOWN' as sku,
        'Unknown product' as product_name,
        'Unknown' as category,
        'Unknown' as subcategory,
        0 as version_number,
        null::numeric(12, 2) as unit_cost,
        null::numeric(12, 2) as unit_price,
        null::numeric(12, 2) as list_margin,
        false as is_active,
        date '1900-01-01' as valid_from,
        null::date as valid_to,
        true as is_current,
        null::date as first_seen_date,
        true as is_unknown_member

)

select
    product_version_key,
    product_key,
    product_id,
    sku,
    product_name,
    category,
    subcategory,
    version_number,
    unit_cost,
    unit_price,
    list_margin,
    is_active,
    valid_from,
    valid_to,
    is_current,
    first_seen_date,
    is_unknown_member
from history

union all

select
    product_version_key,
    product_key,
    product_id,
    sku,
    product_name,
    category,
    subcategory,
    version_number,
    unit_cost,
    unit_price,
    list_margin,
    is_active,
    valid_from,
    valid_to,
    is_current,
    first_seen_date,
    is_unknown_member
from unknown_member
