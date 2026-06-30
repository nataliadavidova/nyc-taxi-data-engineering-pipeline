select
    pickup_date,
    trips_count,
    total_revenue,
    avg_check,
    avg_trip_distance,
    avg_trip_duration_minutes,
    short_trips_count,
    medium_trips_count,
    long_trips_count,
    year,
    month,
    gold_load_timestamp
from {{ source('nyc_taxi_spark_gold', 'gold_daily_trips') }}
