with date_spine as (

    select
        calendar_day::date as full_date
    from generate_series(
        '{{ var("sales_start_date") }}'::timestamp,
        '{{ var("sales_end_date") }}'::timestamp,
        interval '1 day'
    ) as calendar_day

)

select
    {{ date_key('full_date') }} as date_key,
    full_date,
    to_char(full_date, 'FMDay') as day_name,
    to_char(full_date, 'Dy') as day_name_short,
    -- Local business week runs Saturday (1) to Friday (7).
    (extract(isodow from full_date)::integer + 1) % 7 + 1 as day_of_week,
    extract(isodow from full_date)::integer as iso_day_of_week,
    -- Friday is the main weekly day off in Somaliland.
    extract(isodow from full_date) = 5 as is_weekend,
    extract(day from full_date)::integer as day_of_month,
    extract(doy from full_date)::integer as day_of_year,
    extract(week from full_date)::integer as iso_week,
    extract(month from full_date)::integer as month,
    to_char(full_date, 'FMMonth') as month_name,
    to_char(full_date, 'Mon') as month_name_short,
    date_trunc('month', full_date)::date as month_start_date,
    extract(quarter from full_date)::integer as quarter,
    extract(year from full_date)::integer as year,
    to_char(full_date, 'YYYY-MM') as year_month
from date_spine
