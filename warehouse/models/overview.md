{% docs __overview__ %}

# Dukaan Retail data warehouse

A star-schema warehouse for **Dukaan Retail**, a fictional chain of shops in
Somaliland. Nightly point-of-sale extracts are loaded as text into the `raw`
schema by the Python loader, then shaped by dbt in three layers:

| Layer | Schema | Materialization | Purpose |
| --- | --- | --- | --- |
| Staging (`stg_*`) | `staging` | view | Type, trim and deduplicate each source table. One model per source table, no joins. |
| Intermediate (`int_*`) | `intermediate` | view | Join order lines to orders and products and compute line amounts once. |
| Marts (`dim_*`, `fct_*`, `mart_*`) | `marts` | table | Star schema and reporting tables for analysts and BI tools. |

## Data-quality handling

- **Duplicate customers** (exact copies): removed in `stg_customers`.
- **Re-sent orders**: `stg_orders` keeps the version with the latest `updated_at`.
- **Walk-in customers** (no `customer_id`): mapped to the `Walk-in customer` member (key -1) in `dim_customer`.
- **Orphan products** (order lines for products missing from the catalogue): mapped to the `Unknown product` member (key -1) in `dim_product`; their cost and margin stay null rather than being guessed.
- **Empty discounts**: treated as 0.

## Business rules

- Revenue counts **completed** orders only (`fct_sales.is_revenue`). Cancelled and returned lines stay in the fact table for status analysis.
- Cost uses the catalogue's current `unit_cost`, because the source has no cost history.
- Friday is the weekend day (`dim_date.is_weekend`); the local business week runs Saturday to Friday.

{% enddocs %}
