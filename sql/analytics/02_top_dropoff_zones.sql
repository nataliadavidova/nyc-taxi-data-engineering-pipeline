/*
Analytical question:
Identify zones with the highest number of taxi dropoffs.

Source mart:
nyc_taxi.gold_location_pair_stats

Source grain:
One row represents aggregated route statistics by:
- pickup_date
- trip_type
- pickup_location_id
- dropoff_location_id

Methodology:
1. Aggregate the route-level mart to the dropoff zone level.
2. Group by dropoff_borough and dropoff_zone.
3. Sum trips_count to get total dropoff demand for each zone.
4. Sum total_revenue to estimate total revenue associated with trips ending in each zone.
5. Calculate avg_check as a weighted average:
   total_revenue_sum / trips_count_sum

Why weighted average:
Using avg(avg_check) would be misleading because route-level averages have different
trip volumes. Weighted average reflects the actual volume of trips.

Implementation note:
The aggregation is calculated in a subquery first. The outer query calculates
derived metrics and rounding. This avoids nested aggregate expressions and
ClickHouse alias substitution issues.

Limitations:
This query shows where trips end most often. It does not directly show why
passengers travel to these zones.
*/

SELECT
    dropoff_borough,
    dropoff_zone,
    trips_count_sum AS trips_count,
    round(total_revenue_sum, 2) AS total_revenue,
    round(total_revenue_sum / trips_count_sum, 2) AS avg_check
FROM
(
    SELECT
        dropoff_borough,
        dropoff_zone,
        sum(trips_count) AS trips_count_sum,
        sum(total_revenue) AS total_revenue_sum
    FROM nyc_taxi.gold_location_pair_stats
    WHERE dropoff_zone IS NOT NULL
      AND notEmpty(dropoff_zone)
    GROUP BY
        dropoff_borough,
        dropoff_zone
)
ORDER BY trips_count DESC
LIMIT 20