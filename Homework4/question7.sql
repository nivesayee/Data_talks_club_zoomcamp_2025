WITH ranked_trips AS (
    SELECT
        f.pickup_zone,
        z.zone AS dropoff_zone,
        f.p90_trip_duration,
        RANK() OVER (PARTITION BY f.pickup_zone ORDER BY f.p90_trip_duration DESC) AS rank
    FROM fct_fhv_monthly_zone_traveltime_p90 f
    JOIN dim_zones z
        ON f.dropoff_location_id = z.locationid
    WHERE f.year = 2019
        AND f.month = 11
        AND f.pickup_zone IN ('Newark Airport', 'SoHo', 'Yorkville East')
)

SELECT pickup_zone, dropoff_zone, p90_trip_duration
FROM ranked_trips
WHERE rank = 2;