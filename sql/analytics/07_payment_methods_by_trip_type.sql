/*
Analytical question:
Evaluate payment methods for different trip types.

Source mart:
nyc_taxi.gold_payment_type_stats

Source grain:
One row represents aggregated payment statistics by:
- pickup_date
- trip_type
- payment_type
- payment_type_name

Methodology:
1. Use the payment gold mart because it contains both trip_type and payment_type_name.
2. Aggregate data to the level:
   trip_type + payment_type + payment_type_name
3. Sum trips_count to calculate total demand for each payment method within each trip type.
4. Sum total_revenue to calculate total revenue for each payment method within each trip type.
5. Sum total_tips to calculate total tips for each payment method within each trip type.
6. Calculate payment method share within each trip type:
   trips_count_for_payment_type / total_trips_for_trip_type * 100
7. Calculate avg_check as a weighted average:
   total_revenue_sum / trips_count_sum
8. Calculate avg_tip as:
   total_tips_sum / trips_count_sum
9. Calculate tips_share_from_revenue as:
   total_tips_sum / total_revenue_sum * 100

Why weighted metrics:
The source mart is already aggregated by date, trip type, and payment type.
Using simple averages such as avg(avg_check) or avg(avg_tip) would incorrectly
give the same weight to low-volume and high-volume groups.

Business interpretation:
This query helps compare how passengers pay for short, medium, and long trips.
It also helps identify whether card payments dominate across trip types and how
tips differ by trip type and payment method.

Limitations:
Payment type reflects the recorded taxi payment category. It does not directly
explain passenger preference or payment motivation. Some payment categories such
as No charge, Dispute, Unknown, or Voided trip may represent operational or data
quality cases rather than normal customer payment behavior.
*/

WITH
payment_stats AS
(
    SELECT
        trip_type,
        payment_type,
        payment_type_name,
        sum(trips_count) AS trips_count_sum,
        sum(total_revenue) AS total_revenue_sum,
        sum(total_tips) AS total_tips_sum
    FROM nyc_taxi.gold_payment_type_stats
    WHERE trip_type IS NOT NULL
      AND notEmpty(trip_type)
      AND payment_type_name IS NOT NULL
      AND notEmpty(payment_type_name)
    GROUP BY
        trip_type,
        payment_type,
        payment_type_name
),
trip_type_totals AS
(
    SELECT
        trip_type,
        sum(trips_count_sum) AS total_trips_for_trip_type
    FROM payment_stats
    GROUP BY trip_type
)
SELECT
    payment_stats.trip_type,
    payment_stats.payment_type,
    payment_stats.payment_type_name,
    payment_stats.trips_count_sum AS trips_count,
    round(
        payment_stats.trips_count_sum
        / trip_type_totals.total_trips_for_trip_type
        * 100,
        2
    ) AS trips_share_within_type_pct,
    round(payment_stats.total_revenue_sum, 2) AS total_revenue,
    round(payment_stats.total_revenue_sum / payment_stats.trips_count_sum, 2) AS avg_check,
    round(payment_stats.total_tips_sum, 2) AS total_tips,
    round(payment_stats.total_tips_sum / payment_stats.trips_count_sum, 2) AS avg_tip,
    round(
        payment_stats.total_tips_sum
        / nullIf(payment_stats.total_revenue_sum, 0)
        * 100,
        2
    ) AS tips_share_from_revenue_pct
FROM payment_stats
INNER JOIN trip_type_totals
    ON payment_stats.trip_type = trip_type_totals.trip_type
ORDER BY
    multiIf(payment_stats.trip_type = 'short', 1, payment_stats.trip_type = 'medium', 2, payment_stats.trip_type = 'long', 3, 4),
    payment_stats.trips_count_sum DESC