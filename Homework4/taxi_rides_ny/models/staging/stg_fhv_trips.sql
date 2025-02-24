SELECT
    *
FROM {{ source('raw_nyc_tripdata', 'fhv_tripdata_2019') }}
WHERE dispatching_base_num IS NOT NULL;
