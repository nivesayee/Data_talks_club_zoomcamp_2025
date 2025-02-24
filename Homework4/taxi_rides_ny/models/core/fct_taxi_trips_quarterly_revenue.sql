WITH quarterly_revenue AS (
    SELECT
        vendor_id,
        service_type, -- Green or Yellow Taxi
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
        CONCAT(EXTRACT(YEAR FROM pickup_datetime), '/Q', EXTRACT(QUARTER FROM pickup_datetime)) AS year_quarter,
        SUM(total_amount) AS quarterly_revenue
    FROM {{ ref('fact_trips') }}
    GROUP BY 1, 2, 3, 4, 5
),

quarterly_growth AS (
    SELECT
        q1.vendor_id,
        q1.service_type,
        q1.year,
        q1.quarter,
        q1.year_quarter,
        q1.quarterly_revenue,
        q2.quarterly_revenue AS previous_quarter_revenue,
        ROUND(
            100 * (q1.quarterly_revenue - q2.quarterly_revenue) / NULLIF(q2.quarterly_revenue, 0),
            2
        ) AS yoy_growth_percentage
    FROM quarterly_revenue q1
    LEFT JOIN quarterly_revenue q2
        ON q1.vendor_id = q2.vendor_id
        AND q1.service_type = q2.service_type
        AND q1.year = q2.year + 1
        AND q1.quarter = q2.quarter
)

SELECT * FROM quarterly_growth
