-- Databricks SQL dashboard query: Top 5 customers by revenue
-- Recommended visualization: bar chart

SELECT
    CustomerID,
    TotalRevenue
FROM online_retail_pipeline.gold_top_5_customers_by_revenue
ORDER BY TotalRevenue DESC;
