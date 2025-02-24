WITH base AS (
    SELECT
        f.*,
        EXTRACT(YEAR FROM f.pickup_datetime) AS year,
        EXTRACT(MONTH FROM f.pickup_datetime) AS month,
        z1.zone AS pickup_zone,
        z2.zone AS dropoff_zone
    FROM {{ ref('stg_fhv_trips') }} f
    LEFT JOIN {{ ref('dim_zones') }} z1
        ON f.pickup_location_id = z1.locationid
    LEFT JOIN {{ ref('dim_zones') }} z2
        ON f.dropoff_location_id = z2.locationid
)

SELECT * FROM base;
