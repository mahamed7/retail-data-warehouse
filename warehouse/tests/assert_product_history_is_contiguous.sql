-- Product versions must form one unbroken timeline per product:
--   * every version ends exactly where the next one starts (no gaps, no overlaps),
--   * only the last version is open-ended and marked current,
--   * each version starts before it ends.
-- Returns one row per broken version.

with versions as (

    select
        product_key,
        product_version_key,
        version_number,
        valid_from,
        valid_to,
        is_current,
        lead(valid_from) over (partition by product_key order by version_number) as next_valid_from
    from {{ ref('dim_product_history') }}
    where not is_unknown_member

)

select
    product_key,
    product_version_key,
    valid_from,
    valid_to,
    next_valid_from,
    is_current
from versions
where valid_to is distinct from next_valid_from
    or is_current <> (valid_to is null)
    or valid_from >= coalesce(valid_to, date '9999-12-31')
