select
    pickup_date,
    pickup_hour,
    trip_type,
    trips_count,
    total_revenue,
    avg_check,
    avg_trip_distance,
    avg_trip_duration_minutes,
    year,
    month,
    gold_load_timestamp
from {{ source('nyc_taxi_spark_gold', 'gold_hourly_trips') }}
