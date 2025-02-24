WITH trip_durations AS (
    SELECT
        pickup_location_id,
        dropoff_location_id,
        year,
        month,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration
    FROM {{ ref('dim_fhv_trips') }}
)

SELECT
    pickup_location_id,
    dropoff_location_id,
    year,
    month,
    APPROX_QUANTILES(trip_duration, 100)[SAFE_OFFSET(90)] AS p90_trip_duration
FROM trip_durations
GROUP BY pickup_location_id, dropoff_location_id, year, month;
