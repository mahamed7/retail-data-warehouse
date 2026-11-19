-- Type 2 history of each product's price, cost and active flag, rebuilt from the daily
-- catalogue snapshots with a gaps-and-islands pass:
--   1. flag a snapshot when any tracked attribute differs from the previous day's,
--   2. a running sum of the flags numbers the versions,
--   3. each version is valid from its first snapshot day until the next version starts.
-- Because raw keeps every snapshot, this is fully rebuildable and dated by business day
-- (unlike a dbt snapshot, which is dated by when dbt happened to run).

with snapshots as (

    select
        product_id,
        snapshot_date,
        unit_cost,
        unit_price,
        is_active
    from {{ ref('stg_product_snapshots') }}

),

flagged as (

    select
        product_id,
        snapshot_date,
        unit_cost,
        unit_price,
        is_active,
        case
            when lag(unit_cost) over by_day is distinct from unit_cost
                or lag(unit_price) over by_day is distinct from unit_price
                or lag(is_active) over by_day is distinct from is_active
                then 1
            else 0
        end as starts_version
    from snapshots
    window by_day as (partition by product_id order by snapshot_date)

),

numbered as (

    select
        product_id,
        snapshot_date,
        unit_cost,
        unit_price,
        is_active,
        sum(starts_version) over (
            partition by product_id
            order by snapshot_date
            rows between unbounded preceding and current row
        ) as version_number
    from flagged

),

versions as (

    -- Tracked attributes are constant inside a version, so min() just picks the value.
    select
        product_id,
        version_number::integer as version_number,
        min(unit_cost) as unit_cost,
        min(unit_price) as unit_price,
        bool_and(is_active) as is_active,
        min(snapshot_date) as first_seen_date,
        max(snapshot_date) as last_seen_date
    from numbered
    group by product_id, version_number

)

select
    -- Readable surrogate key: product 101, version 2 -> 101002.
    product_id * 1000 + version_number as product_version_key,
    product_id,
    version_number,
    unit_cost,
    unit_price,
    is_active,
    first_seen_date,
    last_seen_date,
    -- History before the first snapshot is unknown; the earliest version covers it.
    case
        when version_number = 1 then date '1900-01-01'
        else first_seen_date
    end as valid_from,
    lead(first_seen_date) over (partition by product_id order by version_number) as valid_to,
    lead(first_seen_date) over (partition by product_id order by version_number) is null as is_current
from versions
