/*
Analytical question:
Evaluate ridesharing opportunities for short trips in nearby zones.

Source mart:
nyc_taxi.gold_location_pair_stats

Source grain:
One row represents aggregated route statistics by:
- pickup_date
- trip_type
- pickup_location_id
- dropoff_location_id

Methodology:
1. Use the route-level gold mart because it contains pickup and dropoff zones.
2. Filter only short trips:
   trip_type = 'short'
3. Use same-borough routes as a proxy for nearby urban movement:
   pickup_borough = dropoff_borough
4. Aggregate data to the route level:
   pickup_borough + pickup_zone + dropoff_borough + dropoff_zone
5. Sum trips_count to calculate annual short-trip demand for each route.
6. Sum total_revenue to calculate annual route revenue.
7. Calculate avg_check as a weighted average:
   total_revenue_sum / trips_count_sum
8. Calculate weighted average trip distance:
   sum(avg_trip_distance * trips_count) / sum(trips_count)
9. Calculate weighted average trip duration:
   sum(avg_trip_duration_minutes * trips_count) / sum(trips_count)

Ridesharing economic model:
- 5% of trips are assumed to be technically groupable.
- 30% of groupable passengers accept grouped rides.
- Grouped ride discount is 5 USD per accepting passenger.
- Passengers who choose individual rides pay an additional 2 USD privacy/time fee.

Calculated metrics:
- potential_groupable_trips = trips_count * 0.05
- grouped_accepting_trips = potential_groupable_trips * 0.30
- individual_privacy_trips = potential_groupable_trips * 0.70
- grouped_discount_cost = grouped_accepting_trips * 5
- privacy_fee_revenue = individual_privacy_trips * 2
- estimated_direct_fare_impact = privacy_fee_revenue - grouped_discount_cost

Interpretation:
The direct fare impact only includes discounts and additional privacy fees.
It does not include operational savings from grouping trips, reduced vehicle miles,
lower emissions, subsidies, increased demand from lower prices, or driver economics.

Limitations:
This is a proxy analysis. The dataset does not contain passenger-level matching data,
real-time pickup coordinates, route overlap geometry, willingness-to-share data, or
vehicle capacity. Same-borough short routes are treated as potential candidates, not
confirmed groupable trips.
*/

WITH
route_stats AS
(
    SELECT
        pickup_borough,
        pickup_zone,
        dropoff_borough,
        dropoff_zone,
        sum(trips_count) AS trips_count_sum,
        sum(total_revenue) AS total_revenue_sum,
        sum(avg_trip_distance * trips_count) AS weighted_trip_distance_sum,
        sum(avg_trip_duration_minutes * trips_count) AS weighted_trip_duration_sum
    FROM nyc_taxi.gold_location_pair_stats
    WHERE trip_type = 'short'
      AND pickup_borough IS NOT NULL
      AND dropoff_borough IS NOT NULL
      AND pickup_zone IS NOT NULL
      AND dropoff_zone IS NOT NULL
      AND notEmpty(pickup_borough)
      AND notEmpty(dropoff_borough)
      AND notEmpty(pickup_zone)
      AND notEmpty(dropoff_zone)
      AND pickup_borough = dropoff_borough
    GROUP BY
        pickup_borough,
        pickup_zone,
        dropoff_borough,
        dropoff_zone
),
candidate_routes AS
(
    SELECT
        pickup_borough,
        pickup_zone,
        dropoff_borough,
        dropoff_zone,
        trips_count_sum AS trips_count,
        round(total_revenue_sum, 2) AS total_revenue,
        round(total_revenue_sum / trips_count_sum, 2) AS avg_check,
        round(weighted_trip_distance_sum / trips_count_sum, 2) AS avg_trip_distance,
        round(weighted_trip_duration_sum / trips_count_sum, 2) AS avg_trip_duration_minutes,
        round(trips_count_sum * 0.05, 0) AS potential_groupable_trips,
        round(trips_count_sum * 0.05 * 0.30, 0) AS grouped_accepting_trips,
        round(trips_count_sum * 0.05 * 0.70, 0) AS individual_privacy_trips,
        round(trips_count_sum * 0.05 * 0.30 * 5, 2) AS grouped_discount_cost,
        round(trips_count_sum * 0.05 * 0.70 * 2, 2) AS privacy_fee_revenue,
        round(
            (trips_count_sum * 0.05 * 0.70 * 2)
            - (trips_count_sum * 0.05 * 0.30 * 5),
            2
        ) AS estimated_direct_fare_impact
    FROM route_stats
)
SELECT
    pickup_borough,
    pickup_zone,
    dropoff_borough,
    dropoff_zone,
    trips_count,
    total_revenue,
    avg_check,
    avg_trip_distance,
    avg_trip_duration_minutes,
    potential_groupable_trips,
    grouped_accepting_trips,
    individual_privacy_trips,
    grouped_discount_cost,
    privacy_fee_revenue,
    estimated_direct_fare_impact
FROM candidate_routes
ORDER BY trips_count DESC
LIMIT 30
