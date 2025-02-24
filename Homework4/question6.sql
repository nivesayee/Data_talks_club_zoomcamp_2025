SELECT
    service_type,
    p97_fare_amount,
    p95_fare_amount,
    p90_fare_amount
FROM fct_taxi_trips_monthly_fare_p95
WHERE year = 2020 AND month = 4;
