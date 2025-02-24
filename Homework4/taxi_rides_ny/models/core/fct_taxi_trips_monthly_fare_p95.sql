WITH filtered_trips AS (
    SELECT
        service_type,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        fare_amount
    FROM {{ ref('fact_trips') }}
    WHERE
        fare_amount > 0
        AND trip_distance > 0
        AND payment_type_description IN ('Cash', 'Credit Card')
)

SELECT
    service_type,
    year,
    month,
    APPROX_QUANTILES(fare_amount, 100)[SAFE_OFFSET(97)] AS p97_fare_amount,
    APPROX_QUANTILES(fare_amount, 100)[SAFE_OFFSET(95)] AS p95_fare_amount,
    APPROX_QUANTILES(fare_amount, 100)[SAFE_OFFSET(90)] AS p90_fare_amount
FROM filtered_trips
GROUP BY service_type, year, month;
