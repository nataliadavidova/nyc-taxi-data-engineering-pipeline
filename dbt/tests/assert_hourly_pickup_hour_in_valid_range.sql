select
    pickup_date,
    pickup_hour,
    trip_type
from {{ ref('stg_hourly_trips') }}
where pickup_hour < 0
   or pickup_hour > 23
