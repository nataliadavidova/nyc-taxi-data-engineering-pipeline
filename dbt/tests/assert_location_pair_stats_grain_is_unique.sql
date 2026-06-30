select
    pickup_date,
    trip_type,
    pickup_location_id,
    dropoff_location_id,
    count() as rows_per_grain
from {{ ref('stg_location_pair_stats') }}
group by
    pickup_date,
    trip_type,
    pickup_location_id,
    dropoff_location_id
having rows_per_grain > 1
