-- Databricks SQL dashboard query: Revenue by country
-- Recommended visualization: bar chart or map

SELECT
    Country,
    TotalRevenue
FROM online_retail_pipeline.gold_revenue_by_country
ORDER BY TotalRevenue DESC;
