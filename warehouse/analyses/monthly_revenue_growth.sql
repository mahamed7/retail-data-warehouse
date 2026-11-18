-- Month-over-month and year-over-year revenue growth for the whole chain, with a
-- three-month moving average to smooth out seasonality.

with monthly_revenue as (

    select
        month_start_date,
        year_month,
        sum(revenue) as revenue
    from {{ ref('mart_monthly_store_performance') }}
    group by
        month_start_date,
        year_month

)

select
    year_month,
    revenue,
    lag(revenue) over (order by month_start_date) as previous_month_revenue,
    round(
        (revenue - lag(revenue) over (order by month_start_date))
        / nullif(lag(revenue) over (order by month_start_date), 0),
        4
    ) as mom_growth_pct,
    round(
        (revenue - lag(revenue, 12) over (order by month_start_date))
        / nullif(lag(revenue, 12) over (order by month_start_date), 0),
        4
    ) as yoy_growth_pct,
    round(
        avg(revenue) over (
            order by month_start_date
            rows between 2 preceding and current row
        ),
        2
    ) as revenue_3m_moving_avg,
    sum(revenue) over (
        partition by date_part('year', month_start_date)
        order by month_start_date
    ) as revenue_year_to_date
from monthly_revenue
order by month_start_date
