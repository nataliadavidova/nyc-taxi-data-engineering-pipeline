select
    pickup_date,
    pickup_month,
    formatDateTime(pickup_date, '%Y-%m') as pickup_month_label,
    pickup_year,
    pickup_month_number,
    day_of_week,
    is_weekend,
    if(is_weekend, 'Weekend', 'Weekday') as day_type,

    trips_count,
    total_revenue,
    round(total_revenue / 1000000, 2) as total_revenue_mln,
    avg_check,
    avg_trip_distance,
    avg_trip_duration_minutes,

    short_trips_count,
    medium_trips_count,
    long_trips_count,

    short_trips_share,
    medium_trips_share,
    long_trips_share,

    multiIf(
        short_trips_share >= medium_trips_share
            and short_trips_share >= long_trips_share,
        'short',

        medium_trips_share >= short_trips_share
            and medium_trips_share >= long_trips_share,
        'medium',

        'long'
    ) as dominant_trip_type

from {{ ref('int_daily_trip_metrics') }}
