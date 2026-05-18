/*
NYC Taxi analytical questions.

These queries answer the business questions from the project requirements.
Run them after the full Airflow DAG has completed successfully.

Database:
nyc_taxi
*/

-- ============================================================================
-- 1. Zones with the highest number of pickups
-- ============================================================================

SELECT
    pickup_borough,
    pickup_zone,
    sum(trips_count) AS trips_count,
    round(sum(total_revenue), 2) AS total_revenue,
    round(avg(avg_check), 2) AS avg_check
FROM nyc_taxi.gold_location_pair_stats
WHERE pickup_zone IS NOT NULL
  AND pickup_zone != ''
GROUP BY
    pickup_borough,
    pickup_zone
ORDER BY trips_count DESC
LIMIT 20;


-- ============================================================================
-- 1. Zones with the highest number of dropoffs
-- ============================================================================

SELECT
    dropoff_borough,
    dropoff_zone,
    sum(trips_count) AS trips_count,
    round(sum(total_revenue), 2) AS total_revenue,
    round(avg(avg_check), 2) AS avg_check
FROM nyc_taxi.gold_location_pair_stats
WHERE dropoff_zone IS NOT NULL
  AND dropoff_zone != ''
GROUP BY
    dropoff_borough,
    dropoff_zone
ORDER BY trips_count DESC
LIMIT 20;


-- ============================================================================
-- 2. Peak taxi demand hours
-- ============================================================================

SELECT
    pickup_hour,
    sum(trips_count) AS trips_count,
    round(sum(total_revenue), 2) AS total_revenue,
    round(avg(avg_check), 2) AS avg_check
FROM nyc_taxi.gold_hourly_trips
GROUP BY pickup_hour
ORDER BY trips_count DESC;


-- ============================================================================
-- 3. Trip distribution by trip type
-- ============================================================================

SELECT
    trip_type,
    trips_count,
    round(trips_count / sum(trips_count) OVER () * 100, 2) AS trips_share_pct
FROM
(
    SELECT
        'short' AS trip_type,
        sum(short_trips_count) AS trips_count
    FROM nyc_taxi.gold_daily_trips

    UNION ALL

    SELECT
        'medium' AS trip_type,
        sum(medium_trips_count) AS trips_count
    FROM nyc_taxi.gold_daily_trips

    UNION ALL

    SELECT
        'long' AS trip_type,
        sum(long_trips_count) AS trips_count
    FROM nyc_taxi.gold_daily_trips
)
ORDER BY trips_count DESC;


-- ============================================================================
-- 4. Peak hours for short, medium, and long trips
-- ============================================================================

SELECT
    trip_type,
    pickup_hour,
    trips_count,
    hour_rank
FROM
(
    SELECT
        trip_type,
        pickup_hour,
        trips_count,
        dense_rank() OVER (
            PARTITION BY trip_type
            ORDER BY trips_count DESC
        ) AS hour_rank
    FROM
    (
        SELECT
            trip_type,
            pickup_hour,
            sum(trips_count) AS trips_count
        FROM nyc_taxi.gold_hourly_trips
        GROUP BY
            trip_type,
            pickup_hour
    )
)
WHERE hour_rank <= 5
ORDER BY
    trip_type,
    hour_rank;


-- ============================================================================
-- 5. Top-3 pickup zones by trip type
-- ============================================================================

SELECT
    trip_type,
    pickup_borough,
    pickup_zone,
    trips_count,
    zone_rank
FROM
(
    SELECT
        trip_type,
        pickup_borough,
        pickup_zone,
        trips_count,
        dense_rank() OVER (
            PARTITION BY trip_type
            ORDER BY trips_count DESC
        ) AS zone_rank
    FROM
    (
        SELECT
            trip_type,
            pickup_borough,
            pickup_zone,
            sum(trips_count) AS trips_count
        FROM nyc_taxi.gold_location_pair_stats
        WHERE pickup_zone IS NOT NULL
          AND pickup_zone != ''
        GROUP BY
            trip_type,
            pickup_borough,
            pickup_zone
    )
)
WHERE zone_rank <= 3
ORDER BY
    trip_type,
    zone_rank;


-- ============================================================================
-- 5. Top-3 dropoff zones by trip type
-- ============================================================================

SELECT
    trip_type,
    dropoff_borough,
    dropoff_zone,
    trips_count,
    zone_rank
FROM
(
    SELECT
        trip_type,
        dropoff_borough,
        dropoff_zone,
        trips_count,
        dense_rank() OVER (
            PARTITION BY trip_type
            ORDER BY trips_count DESC
        ) AS zone_rank
    FROM
    (
        SELECT
            trip_type,
            dropoff_borough,
            dropoff_zone,
            sum(trips_count) AS trips_count
        FROM nyc_taxi.gold_location_pair_stats
        WHERE dropoff_zone IS NOT NULL
          AND dropoff_zone != ''
        GROUP BY
            trip_type,
            dropoff_borough,
            dropoff_zone
    )
)
WHERE zone_rank <= 3
ORDER BY
    trip_type,
    zone_rank;


-- ============================================================================
-- 6. Payment methods by trip type
-- ============================================================================

SELECT
    trip_type,
    payment_type_name,
    trips_count,
    round(trips_count / sum(trips_count) OVER (PARTITION BY trip_type) * 100, 2)
        AS payment_share_pct,
    round(total_revenue, 2) AS total_revenue,
    round(total_tips, 2) AS total_tips
FROM
(
    SELECT
        trip_type,
        payment_type_name,
        sum(trips_count) AS trips_count,
        sum(total_revenue) AS total_revenue,
        sum(total_tips) AS total_tips
    FROM nyc_taxi.gold_payment_type_stats
    GROUP BY
        trip_type,
        payment_type_name
)
ORDER BY
    trip_type,
    trips_count DESC;


-- ============================================================================
-- 7. Payment preference evolution over time
-- ============================================================================

SELECT
    pickup_month,
    payment_type_name,
    trips_count,
    round(trips_count / sum(trips_count) OVER (PARTITION BY pickup_month) * 100, 2)
        AS payment_share_pct
FROM
(
    SELECT
        toStartOfMonth(pickup_date) AS pickup_month,
        payment_type_name,
        sum(trips_count) AS trips_count
    FROM nyc_taxi.gold_payment_type_stats
    GROUP BY
        pickup_month,
        payment_type_name
)
ORDER BY
    pickup_month,
    trips_count DESC;


-- ============================================================================
-- 7. Payment preference evolution over time by trip type
-- ============================================================================

SELECT
    pickup_month,
    trip_type,
    payment_type_name,
    trips_count,
    round(
        trips_count / sum(trips_count) OVER (PARTITION BY pickup_month, trip_type) * 100,
        2
    ) AS payment_share_pct
FROM
(
    SELECT
        toStartOfMonth(pickup_date) AS pickup_month,
        trip_type,
        payment_type_name,
        sum(trips_count) AS trips_count
    FROM nyc_taxi.gold_payment_type_stats
    GROUP BY
        pickup_month,
        trip_type,
        payment_type_name
)
ORDER BY
    pickup_month,
    trip_type,
    trips_count DESC;


-- ============================================================================
-- 8. Ridesharing opportunity: top short nearby routes
-- ============================================================================

SELECT
    pickup_borough,
    pickup_zone,
    dropoff_borough,
    dropoff_zone,
    sum(trips_count) AS short_trips_count,
    round(sum(total_revenue), 2) AS total_revenue,
    round(avg(avg_trip_distance), 2) AS avg_trip_distance,
    round(avg(avg_trip_duration_minutes), 2) AS avg_trip_duration_minutes
FROM nyc_taxi.gold_location_pair_stats
WHERE trip_type = 'short'
  AND pickup_zone IS NOT NULL
  AND pickup_zone != ''
  AND dropoff_zone IS NOT NULL
  AND dropoff_zone != ''
  AND pickup_zone != dropoff_zone
  AND avg_trip_distance <= 2
GROUP BY
    pickup_borough,
    pickup_zone,
    dropoff_borough,
    dropoff_zone
ORDER BY short_trips_count DESC
LIMIT 30;


-- ============================================================================
-- 8. Simplified economic impact model for grouped short trips
--
-- Assumptions:
-- - 5% of eligible short nearby trips can be grouped
-- - 30% of passengers accept grouped rides
-- - $5 discount is provided for grouped rides
-- - $2 privacy fee is charged when passengers keep an individual ride
-- ============================================================================

WITH
    0.05 AS groupable_share,
    0.30 AS adoption_share,
    5.0 AS group_discount,
    2.0 AS privacy_fee
SELECT
    eligible_short_trips,
    round(eligible_short_trips * groupable_share, 0) AS groupable_trips,
    round(eligible_short_trips * groupable_share * adoption_share, 0)
        AS accepted_group_trips,
    round(eligible_short_trips * groupable_share * (1 - adoption_share), 0)
        AS declined_group_trips,
    round(eligible_short_trips * groupable_share * adoption_share * group_discount, 2)
        AS discount_cost,
    round(eligible_short_trips * groupable_share * (1 - adoption_share) * privacy_fee, 2)
        AS privacy_fee_revenue,
    round(
        eligible_short_trips * groupable_share * (1 - adoption_share) * privacy_fee
        - eligible_short_trips * groupable_share * adoption_share * group_discount,
        2
    ) AS estimated_net_effect
FROM
(
    SELECT
        sum(trips_count) AS eligible_short_trips
    FROM nyc_taxi.gold_location_pair_stats
    WHERE trip_type = 'short'
      AND pickup_zone IS NOT NULL
      AND pickup_zone != ''
      AND dropoff_zone IS NOT NULL
      AND dropoff_zone != ''
      AND pickup_zone != dropoff_zone
      AND avg_trip_distance <= 2
);
