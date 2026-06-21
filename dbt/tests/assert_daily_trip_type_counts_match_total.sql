select
    pickup_date,
    trips_count,
    short_trips_count,
    medium_trips_count,
    long_trips_count,
    short_trips_count
        + medium_trips_count
        + long_trips_count as classified_trips_count
from {{ ref('stg_daily_trips') }}
where short_trips_count
    + medium_trips_count
    + long_trips_count
    != trips_count
