/*
Analytical question:
Track payment preference evolution over time.

Source mart:
nyc_taxi.gold_payment_type_stats

Source grain:
One row represents aggregated payment statistics by:
- pickup_date
- trip_type
- payment_type
- payment_type_name

Methodology:
1. Use the payment gold mart because it contains pickup_date, payment_type_name, and trips_count.
2. Aggregate data to the monthly payment method level:
   pickup_month + payment_type + payment_type_name
3. Sum trips_count to calculate monthly trip volume for each payment method.
4. Sum total_revenue to calculate monthly revenue for each payment method.
5. Sum total_tips to calculate monthly tips for each payment method.
6. Calculate monthly payment method share:
   payment_method_monthly_trips / total_monthly_trips * 100
7. Calculate avg_check as a weighted average:
   total_revenue_sum / trips_count_sum
8. Calculate avg_tip as:
   total_tips_sum / trips_count_sum
9. Calculate tips_share_from_revenue as:
   total_tips_sum / total_revenue_sum * 100

Why monthly aggregation:
The analytical question is about payment preference evolution over time.
Monthly aggregation smooths daily volatility and makes long-term trends easier to read.

Why weighted metrics:
The source mart is already aggregated by date, trip type, and payment type.
Using simple averages such as avg(avg_check) or avg(avg_tip) would incorrectly
give the same weight to low-volume and high-volume groups.

Business interpretation:
This query helps evaluate whether passengers are shifting toward or away from
specific payment methods over time. It is especially useful for monitoring
credit card dependency, cash usage decline, and non-standard payment categories.

Limitations:
Payment type reflects the recorded taxi payment category. It does not directly
explain passenger motivation. Cash tips may not be fully captured in the tip_amount field.
*/

WITH
monthly_payment_stats AS
(
    SELECT
        toStartOfMonth(pickup_date) AS pickup_month,
        payment_type,
        payment_type_name,
        sum(trips_count) AS trips_count_sum,
        sum(total_revenue) AS total_revenue_sum,
        sum(total_tips) AS total_tips_sum
    FROM nyc_taxi.gold_payment_type_stats
    WHERE payment_type_name IS NOT NULL
      AND notEmpty(payment_type_name)
    GROUP BY
        pickup_month,
        payment_type,
        payment_type_name
),
monthly_totals AS
(
    SELECT
        pickup_month,
        sum(trips_count_sum) AS total_monthly_trips
    FROM monthly_payment_stats
    GROUP BY pickup_month
)
SELECT
    monthly_payment_stats.pickup_month,
    monthly_payment_stats.payment_type,
    monthly_payment_stats.payment_type_name,
    monthly_payment_stats.trips_count_sum AS trips_count,
    round(
        monthly_payment_stats.trips_count_sum
        / monthly_totals.total_monthly_trips
        * 100,
        2
    ) AS trips_share_month_pct,
    round(monthly_payment_stats.total_revenue_sum, 2) AS total_revenue,
    round(
        monthly_payment_stats.total_revenue_sum
        / monthly_payment_stats.trips_count_sum,
        2
    ) AS avg_check,
    round(monthly_payment_stats.total_tips_sum, 2) AS total_tips,
    round(
        monthly_payment_stats.total_tips_sum
        / monthly_payment_stats.trips_count_sum,
        2
    ) AS avg_tip,
    round(
        monthly_payment_stats.total_tips_sum
        / nullIf(monthly_payment_stats.total_revenue_sum, 0)
        * 100,
        2
    ) AS tips_share_from_revenue_pct
FROM monthly_payment_stats
INNER JOIN monthly_totals
    ON monthly_payment_stats.pickup_month = monthly_totals.pickup_month
ORDER BY
    monthly_payment_stats.pickup_month,
    multiIf(
        monthly_payment_stats.payment_type_name = 'Credit card', 1,
        monthly_payment_stats.payment_type_name = 'Cash', 2,
        monthly_payment_stats.payment_type_name = 'Other', 3,
        monthly_payment_stats.payment_type_name = 'Dispute', 4,
        monthly_payment_stats.payment_type_name = 'No charge', 5,
        6
    )