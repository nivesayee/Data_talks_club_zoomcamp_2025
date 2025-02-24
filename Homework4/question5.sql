WITH ranked_growth AS (
    SELECT
        taxi_type,
        year_quarter,
        YoY_growth_percentage,
        RANK() OVER (PARTITION BY taxi_type ORDER BY YoY_growth_percentage DESC) AS best_rank,
        RANK() OVER (PARTITION BY taxi_type ORDER BY YoY_growth_percentage ASC) AS worst_rank
    FROM fct_taxi_trips_quarterly_revenue
    WHERE year = 2020
)
SELECT
    taxi_type,
    MAX(CASE WHEN best_rank = 1 THEN year_quarter END) AS best_quarter,
    MAX(CASE WHEN worst_rank = 1 THEN year_quarter END) AS worst_quarter
FROM ranked_growth
GROUP BY taxi_type;
