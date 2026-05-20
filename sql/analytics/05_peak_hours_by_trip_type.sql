/*
Analytical question:
Identify peak taxi demand hours for each trip type: short, medium, and long.

Source mart:
nyc_taxi.gold_hourly_trips

Source grain:
One row represents aggregated hourly trip statistics by:
- pickup_date
- pickup_hour
- trip_type

Methodology:
1. Aggregate the hourly gold mart to the trip_type + pickup_hour level.
2. Group by trip_type and pickup_hour.
3. Sum trips_count to calculate hourly demand for each trip type.
4. Sum total_revenue to calculate hourly revenue for each trip type.
5. Calculate each hour's share within its trip type:
   trips_count_for_trip_type_hour / total_trips_for_trip_type * 100
6. Calculate avg_check as a weighted average:
   total_revenue_sum / trips_count_sum
7. Calculate weighted average trip distance:
   sum(avg_trip_distance * trips_count) / sum(trips_count)
8. Calculate weighted average trip duration:
   sum(avg_trip_duration_minutes * trips_count) / sum(trips_count)
9. Rank hours separately inside each trip_type by trips_count.

Why ranking is partitioned by trip_type:
The goal is to identify peak hours for each trip type separately.
For example, short trips and long trips may have different demand patterns.
Therefore, ranking is calculated independently for short, medium, and long trips.

Why weighted averages:
The source mart is already aggregated by date, hour, and trip type.
Using simple averages such as avg(avg_check), avg(avg_trip_distance), or
avg(avg_trip_duration_minutes) would incorrectly give the same weight to
low-volume and high-volume hourly groups.

Limitations:
This query identifies peak hours by trip type across the full year.
It does not separate weekdays from weekends, holidays, weather conditions,
airport-specific patterns, or special events.
*/

WITH
hour_trip_type_stats AS
(
    SELECT
        trip_type,
        pickup_hour,
        sum(trips_count) AS trips_count_sum,
        sum(total_revenue) AS total_revenue_sum,
        sum(avg_trip_distance * trips_count) AS weighted_trip_distance_sum,
        sum(avg_trip_duration_minutes * trips_count) AS weighted_trip_duration_sum
    FROM nyc_taxi.gold_hourly_trips
    WHERE trip_type IS NOT NULL
      AND notEmpty(trip_type)
    GROUP BY
        trip_type,
        pickup_hour
),
trip_type_totals AS
(
    SELECT
        trip_type,
        sum(trips_count_sum) AS total_trips_for_trip_type
    FROM hour_trip_type_stats
    GROUP BY trip_type
),
ranked_hours AS
(
    SELECT
        hour_trip_type_stats.trip_type,
        hour_trip_type_stats.pickup_hour,
        hour_trip_type_stats.trips_count_sum AS trips_count,
        round(
            hour_trip_type_stats.trips_count_sum
            / trip_type_totals.total_trips_for_trip_type
            * 100,
            2
        ) AS trips_share_within_type_pct,
        round(hour_trip_type_stats.total_revenue_sum, 2) AS total_revenue,
        round(
            hour_trip_type_stats.total_revenue_sum
            / hour_trip_type_stats.trips_count_sum,
            2
        ) AS avg_check,
        round(
            hour_trip_type_stats.weighted_trip_distance_sum
            / hour_trip_type_stats.trips_count_sum,
            2
        ) AS avg_trip_distance,
        round(
            hour_trip_type_stats.weighted_trip_duration_sum
            / hour_trip_type_stats.trips_count_sum,
            2
        ) AS avg_trip_duration_minutes,
        row_number() OVER (
            PARTITION BY hour_trip_type_stats.trip_type
            ORDER BY hour_trip_type_stats.trips_count_sum DESC
        ) AS hour_rank_within_type
    FROM hour_trip_type_stats
    INNER JOIN trip_type_totals
        ON hour_trip_type_stats.trip_type = trip_type_totals.trip_type
)
SELECT
    trip_type,
    hour_rank_within_type,
    pickup_hour,
    trips_count,
    trips_share_within_type_pct,
    total_revenue,
    avg_check,
    avg_trip_distance,
    avg_trip_duration_minutes
FROM ranked_hours
WHERE hour_rank_within_type <= 10
ORDER BY
    multiIf(trip_type = 'short', 1, trip_type = 'medium', 2, trip_type = 'long', 3, 4),
    hour_rank_within_type