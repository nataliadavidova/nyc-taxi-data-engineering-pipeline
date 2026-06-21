select
    pickup_date,
    toStartOfMonth(pickup_date) as pickup_month,
    toYear(pickup_date) as pickup_year,
    toMonth(pickup_date) as pickup_month_number,
    toDayOfWeek(pickup_date) as day_of_week,
    toDayOfWeek(pickup_date) in (6, 7) as is_weekend,

    trips_count,
    total_revenue,
    avg_check,
    avg_trip_distance,
    avg_trip_duration_minutes,

    short_trips_count,
    medium_trips_count,
    long_trips_count,

    if(trips_count = 0, 0, round(short_trips_count / trips_count, 4)) as short_trips_share,
    if(trips_count = 0, 0, round(medium_trips_count / trips_count, 4)) as medium_trips_share,
    if(trips_count = 0, 0, round(long_trips_count / trips_count, 4)) as long_trips_share,

    gold_load_timestamp
from {{ ref('stg_daily_trips') }}
