select
    pickup_date,
    trip_type,
    payment_type,
    payment_type_name,
    trips_count,
    total_revenue,
    avg_check,
    total_tips,
    avg_tip,
    tips_share_from_revenue,
    year,
    month,
    gold_load_timestamp
from {{ source('nyc_taxi_spark_gold', 'gold_payment_type_stats') }}
