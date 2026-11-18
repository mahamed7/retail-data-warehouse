with stores as (

    select
        store_key,
        store_name,
        city,
        region,
        store_type
    from {{ ref('dim_store') }}

),

months as (

    select distinct
        month_start_date,
        year_month,
        year,
        month
    from {{ ref('dim_date') }}

),

sales as (

    select
        fct_sales.store_key,
        dim_date.month_start_date,
        fct_sales.order_id,
        fct_sales.order_status,
        fct_sales.is_revenue,
        fct_sales.quantity,
        fct_sales.net_amount,
        fct_sales.gross_margin
    from {{ ref('fct_sales') }} as fct_sales
    inner join {{ ref('dim_date') }} as dim_date
        on fct_sales.date_key = dim_date.date_key

),

monthly_sales as (

    select
        store_key,
        month_start_date,
        count(distinct order_id) as total_orders,
        count(distinct order_id) filter (where order_status = 'completed') as completed_orders,
        count(distinct order_id) filter (where order_status = 'cancelled') as cancelled_orders,
        count(distinct order_id) filter (where order_status = 'returned') as returned_orders,
        sum(quantity) filter (where is_revenue) as units_sold,
        sum(net_amount) filter (where is_revenue) as revenue,
        sum(gross_margin) filter (where is_revenue) as gross_margin,
        -- Margin % is measured only on lines with a known cost (orphan products have none).
        sum(net_amount) filter (where is_revenue and gross_margin is not null) as revenue_with_known_cost
    from sales
    group by
        store_key,
        month_start_date

),

-- Every store gets a row for every month, even a month with no sales.
store_months as (

    select
        stores.store_key,
        stores.store_name,
        stores.city,
        stores.region,
        stores.store_type,
        months.month_start_date,
        months.year_month,
        months.year,
        months.month,
        coalesce(monthly_sales.total_orders, 0)::integer as total_orders,
        coalesce(monthly_sales.completed_orders, 0)::integer as completed_orders,
        coalesce(monthly_sales.cancelled_orders, 0)::integer as cancelled_orders,
        coalesce(monthly_sales.returned_orders, 0)::integer as returned_orders,
        coalesce(monthly_sales.units_sold, 0)::integer as units_sold,
        coalesce(monthly_sales.revenue, 0)::numeric(14, 2) as revenue,
        coalesce(monthly_sales.gross_margin, 0)::numeric(14, 2) as gross_margin,
        monthly_sales.revenue_with_known_cost
    from stores
    cross join months
    left join monthly_sales
        on stores.store_key = monthly_sales.store_key
        and months.month_start_date = monthly_sales.month_start_date

)

select
    store_key,
    store_name,
    city,
    region,
    store_type,
    month_start_date,
    year_month,
    year,
    month,
    total_orders,
    completed_orders,
    cancelled_orders,
    returned_orders,
    units_sold,
    revenue,
    gross_margin,
    round(gross_margin / nullif(revenue_with_known_cost, 0), 4) as margin_pct,
    round(revenue / nullif(completed_orders, 0), 2) as avg_order_value,
    round(returned_orders::numeric / nullif(completed_orders + returned_orders, 0), 4) as returned_rate
from store_months
