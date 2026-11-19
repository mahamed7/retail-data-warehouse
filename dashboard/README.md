# Dashboard

A static sales dashboard for Dukaan Retail, built from the warehouse marts and published
to GitHub Pages by CI. No build step: `index.html`, `app.js` and `styles.css` fetch the
JSON in `data/` and draw everything in the browser (Chart.js 4.4.1 from cdnjs for the
monthly chart; everything else is plain HTML and SVG).

## What it shows

| Panel | Source |
| --- | --- |
| KPI cards: revenue, gross margin %, completed orders, average order value, return rate, walk-in share, revenue growth 2025 vs 2024 | `fct_sales` + `dim_date` |
| Monthly revenue, margin % or orders, 2025 against 2024 (with a table view) | `fct_sales` + `dim_date` |
| Store league table, sortable, with a 24-month revenue sparkline per store | `fct_sales`, `dim_store`, `mart_monthly_store_performance` |
| Revenue by category, with share and margin | `fct_sales` + `dim_product` |
| Channel × payment method mix | `fct_sales` |
| Data quality handled: duplicate customers removed, re-sent orders collapsed, walk-in orders, orphan product lines, blank discounts, emails cleaned | `raw.*` compared with `staging.*` and `fct_sales` |

The year filter (All, 2024, 2025) updates every panel except data quality, which describes
the whole run. Revenue, margin and AOV count completed orders only (`is_revenue`). Light
and dark themes follow the system setting and can be switched with the button in the header.

## How the data gets here

`export_data.py` runs read-only SQL against the warehouse and writes compact JSON to
`data/`:

```bash
python pipeline/generate_data.py
python pipeline/load_raw.py
(cd warehouse && dbt build --profiles-dir .)
python dashboard/export_data.py            # --out to write somewhere else
```

It connects with the standard libpq variables (`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`,
`PGDATABASE`; defaults `localhost`, `5432`, `postgres`, `postgres`, `retail`) and stamps
`meta.json` with the build time and, in CI, the commit from `GITHUB_SHA`.

The JSON committed in `data/` is a small sample computed from the same synthetic data
(`meta.json` says `"source": "sample"`), so the page works before CI has run. CI overwrites
it on every build.

## Run it locally

```bash
cd dashboard
python -m http.server 8000
```

Then open <http://localhost:8000>. Opening `index.html` straight from disk won't work,
because browsers block `fetch` on `file://` pages.
