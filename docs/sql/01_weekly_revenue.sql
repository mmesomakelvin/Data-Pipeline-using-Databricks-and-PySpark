-- Databricks SQL dashboard query: Weekly revenue
-- Recommended visualization: line chart

SELECT
    WeekStartDate,
    WeeklyRevenue
FROM online_retail_pipeline.gold_weekly_revenue
ORDER BY WeekStartDate;
