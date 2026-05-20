/*
Analytical question:
Rank the top 3 pickup and dropoff zones for each trip type.

Source mart:
nyc_taxi.gold_location_pair_stats

Source grain:
One row represents aggregated route statistics by:
- pickup_date
- trip_type
- pickup_location_id
- dropoff_location_id

Methodology:
1. Use the route-level gold mart because it contains both pickup/dropoff zones and trip_type.
2. Aggregate pickup demand to the level:
   trip_type + pickup_borough + pickup_zone
3. Aggregate dropoff demand to the level:
   trip_type + dropoff_borough + dropoff_zone
4. Sum trips_count to calculate total demand for each zone within each trip type.
5. Sum total_revenue to calculate total revenue for each zone within each trip type.
6. Calculate avg_check as a weighted average:
   total_revenue_sum / trips_count_sum
7. Calculate weighted average trip distance:
   sum(avg_trip_distance * trips_count) / sum(trips_count)
8. Calculate weighted average trip duration:
   sum(avg_trip_duration_minutes * trips_count) / sum(trips_count)
9. Rank zones separately by:
   - zone role: pickup or dropoff
   - trip_type: short, medium, long
10. Keep only the top 3 zones for each zone role and trip type.

Why weighted averages:
The source mart is already aggregated by date, trip type, pickup zone, and dropoff zone.
Using simple averages such as avg(avg_check), avg(avg_trip_distance), or
avg(avg_trip_duration_minutes) would incorrectly give the same weight to
low-volume and high-volume route groups.

Business interpretation:
This query helps identify which zones dominate demand for short, medium, and long trips.
It is useful for driver allocation, zone-based pricing, short-trip promotions,
airport flow monitoring, and grouped ride opportunity analysis.

Limitations:
The query ranks zones by trip volume. A zone with fewer trips may still be more valuable
by revenue or average check. Passenger motivation is inferred indirectly and should be
validated with additional context such as POI, airport flags, events, and time of day.
*/

WITH
pickup_stats AS
(
    SELECT
        'pickup' AS zone_role,
        trip_type,
        pickup_borough AS borough,
        pickup_zone AS zone,
        sum(trips_count) AS trips_count_sum,
        sum(total_revenue) AS total_revenue_sum,
        sum(avg_trip_distance * trips_count) AS weighted_trip_distance_sum,
        sum(avg_trip_duration_minutes * trips_count) AS weighted_trip_duration_sum
    FROM nyc_taxi.gold_location_pair_stats
    WHERE trip_type IS NOT NULL
      AND notEmpty(trip_type)
      AND pickup_zone IS NOT NULL
      AND notEmpty(pickup_zone)
    GROUP BY
        trip_type,
        pickup_borough,
        pickup_zone
),
dropoff_stats AS
(
    SELECT
        'dropoff' AS zone_role,
        trip_type,
        dropoff_borough AS borough,
        dropoff_zone AS zone,
        sum(trips_count) AS trips_count_sum,
        sum(total_revenue) AS total_revenue_sum,
        sum(avg_trip_distance * trips_count) AS weighted_trip_distance_sum,
        sum(avg_trip_duration_minutes * trips_count) AS weighted_trip_duration_sum
    FROM nyc_taxi.gold_location_pair_stats
    WHERE trip_type IS NOT NULL
      AND notEmpty(trip_type)
      AND dropoff_zone IS NOT NULL
      AND notEmpty(dropoff_zone)
    GROUP BY
        trip_type,
        dropoff_borough,
        dropoff_zone
),
combined_stats AS
(
    SELECT * FROM pickup_stats
    UNION ALL
    SELECT * FROM dropoff_stats
),
ranked_zones AS
(
    SELECT
        zone_role,
        trip_type,
        borough,
        zone,
        trips_count_sum AS trips_count,
        round(total_revenue_sum, 2) AS total_revenue,
        round(total_revenue_sum / trips_count_sum, 2) AS avg_check,
        round(weighted_trip_distance_sum / trips_count_sum, 2) AS avg_trip_distance,
        round(weighted_trip_duration_sum / trips_count_sum, 2) AS avg_trip_duration_minutes,
        row_number() OVER (
            PARTITION BY zone_role, trip_type
            ORDER BY trips_count_sum DESC
        ) AS zone_rank
    FROM combined_stats
)
SELECT
    zone_role,
    trip_type,
    zone_rank,
    borough,
    zone,
    trips_count,
    total_revenue,
    avg_check,
    avg_trip_distance,
    avg_trip_duration_minutes
FROM ranked_zones
WHERE zone_rank <= 3
ORDER BY
    multiIf(zone_role = 'pickup', 1, zone_role = 'dropoff', 2, 3),
    multiIf(trip_type = 'short', 1, trip_type = 'medium', 2, trip_type = 'long', 3, 4),
    zone_rank