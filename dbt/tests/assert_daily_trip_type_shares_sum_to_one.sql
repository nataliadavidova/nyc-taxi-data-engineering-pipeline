select
    pickup_date,
    trips_count,
    short_trips_share,
    medium_trips_share,
    long_trips_share,
    short_trips_share
        + medium_trips_share
        + long_trips_share as total_trip_type_share
from {{ ref('int_daily_trip_metrics') }}
where trips_count > 0
  and abs(
      short_trips_share
      + medium_trips_share
      + long_trips_share
      - 1
  ) > 0.001
