# Databricks SQL Dashboard Queries

These queries are for Databricks SQL dashboards. PostgreSQL is not required for this project.

Use one query per dashboard visualization:

```text
01_weekly_revenue.sql
02_top_5_products_by_revenue.sql
03_top_5_customers_by_revenue.sql
04_revenue_by_country.sql
```

Expected dashboard visuals:

1. Weekly revenue: line chart
2. Top five products by revenue: bar chart
3. Top five customers by revenue: bar chart
4. Revenue by country: bar chart or map

All queries read from Gold Delta tables in the `online_retail_pipeline` database.
