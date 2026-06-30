select
    pickup_date,
    trip_type,
    payment_type,
    count() as rows_per_grain
from {{ ref('stg_payment_type_stats') }}
group by
    pickup_date,
    trip_type,
    payment_type
having rows_per_grain > 1
