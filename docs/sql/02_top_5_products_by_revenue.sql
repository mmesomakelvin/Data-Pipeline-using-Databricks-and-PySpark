-- Databricks SQL dashboard query: Top 5 products by revenue
-- Recommended visualization: bar chart

SELECT
    StockCode,
    Description,
    TotalRevenue,
    TotalQuantitySold
FROM online_retail_pipeline.gold_top_5_products_by_revenue
ORDER BY TotalRevenue DESC;
