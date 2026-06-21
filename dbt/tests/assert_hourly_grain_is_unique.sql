select
    pickup_date,
    pickup_hour,
    trip_type,
    count() as rows_per_grain
from {{ ref('stg_hourly_trips') }}
group by
    pickup_date,
    pickup_hour,
    trip_type
having rows_per_grain > 1
