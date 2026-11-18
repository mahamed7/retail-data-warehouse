-- Which products earn the most gross margin, and how efficiently?
-- Completed orders only; the Unknown product is left out because it has no cost.

select
    dim_product.product_name,
    dim_product.category,
    dim_product.subcategory,
    sum(fct_sales.quantity) as units_sold,
    sum(fct_sales.net_amount) as revenue,
    sum(fct_sales.gross_margin) as gross_margin,
    round(sum(fct_sales.gross_margin) / nullif(sum(fct_sales.net_amount), 0), 4) as margin_pct,
    rank() over (order by sum(fct_sales.gross_margin) desc) as margin_rank
from {{ ref('fct_sales') }} as fct_sales
inner join {{ ref('dim_product') }} as dim_product
    on fct_sales.product_key = dim_product.product_key
where fct_sales.is_revenue
    and not dim_product.is_unknown_member
group by
    dim_product.product_name,
    dim_product.category,
    dim_product.subcategory
order by
    margin_rank,
    dim_product.product_name
limit 10
