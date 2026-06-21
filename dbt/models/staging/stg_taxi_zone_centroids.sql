select
    location_id,
    borough,
    zone,
    longitude,
    latitude
from {{ source('nyc_taxi_spark_gold', 'taxi_zone_centroids') }}
