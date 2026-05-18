# NYC Taxi Analytical Summary

This document summarizes the analytical findings for the NYC Yellow Taxi data engineering project.

The analysis is based on the full year 2024 and uses gold marts loaded into ClickHouse.

## Analytical Questions

### 1. Zones with the Highest Pickup and Dropoff Demand

**Question:**  
Which zones have the highest number of taxi pickups and dropoffs?

**Data source:**  
`nyc_taxi.gold_location_pair_stats`

**Planned analysis:**

- rank pickup zones by total trips;
- rank dropoff zones by total trips;
- compare zone demand by borough;
- identify whether demand is concentrated in specific NYC areas.

**Key findings:**  
_To be filled after running analytical SQL queries._

**Business interpretation:**  
_To be filled after reviewing results._

---

### 2. Peak Taxi Demand Hours

**Question:**  
What are the peak hours for taxi trips?

**Data source:**  
`nyc_taxi.gold_hourly_trips`

**Planned analysis:**

- aggregate trips by pickup hour;
- identify hours with the highest trip volume;
- compare demand patterns across the day.

**Key findings:**  
_To be filled after running analytical SQL queries._

**Business interpretation:**  
_To be filled after reviewing results._

---

### 3. Trip Distribution and Possible Passenger Behavior Patterns

**Question:**  
How are trips distributed by trip type, and what passenger behavior patterns can be inferred?

**Data sources:**

- `nyc_taxi.gold_daily_trips`
- `nyc_taxi.gold_hourly_trips`
- `nyc_taxi.gold_location_pair_stats`

**Planned analysis:**

- compare short, medium, and long trip volumes;
- analyze time-of-day patterns by trip type;
- analyze popular zones and routes by trip type.

**Important note:**  
The dataset does not directly contain passenger motives. Any interpretation of motives is based on observable patterns such as trip length, time of day, and location.

**Key findings:**  
_To be filled after running analytical SQL queries._

**Business interpretation:**  
_To be filled after reviewing results._

---

### 4. Peak Hours for Short, Medium, and Long Trips

**Question:**  
What are the peak hours for different trip types?

**Data source:**  
`nyc_taxi.gold_hourly_trips`

**Planned analysis:**

- rank pickup hours separately for `short`, `medium`, and `long` trips;
- compare whether short and long trips peak at different times;
- identify opportunities for time-based pricing or promotions.

**Key findings:**  
_To be filled after running analytical SQL queries._

**Business interpretation:**  
_To be filled after reviewing results._

---

### 5. Top Pickup and Dropoff Zones by Trip Type

**Question:**  
What are the top pickup and dropoff zones for different trip types?

**Data source:**  
`nyc_taxi.gold_location_pair_stats`

**Planned analysis:**

- rank top pickup zones by `trip_type`;
- rank top dropoff zones by `trip_type`;
- identify zones where short, medium, or long trips dominate.

**Key findings:**  
_To be filled after running analytical SQL queries._

**Business interpretation:**  
_To be filled after reviewing results._

---

### 6. Payment Methods by Trip Type

**Question:**  
How do payment methods differ across trip types?

**Data source:**  
`nyc_taxi.gold_payment_type_stats`

**Planned analysis:**

- compare payment method shares for short, medium, and long trips;
- compare revenue and tips by payment method and trip type;
- identify whether card or cash usage differs by trip length.

**Key findings:**  
_To be filled after running analytical SQL queries._

**Business interpretation:**  
_To be filled after reviewing results._

---

### 7. Payment Preference Evolution Over Time

**Question:**  
How did payment preferences change over time during 2024?

**Data source:**  
`nyc_taxi.gold_payment_type_stats`

**Planned analysis:**

- aggregate payment methods by month;
- calculate monthly payment method shares;
- identify whether cash payments became less popular over time;
- optionally analyze monthly payment trends by trip type.

**Key findings:**  
_To be filled after running analytical SQL queries._

**Business interpretation:**  
_To be filled after reviewing results._

---

### 8. Ridesharing Opportunity for Short Nearby Trips

**Question:**  
Is there an opportunity to group short nearby trips into shared rides?

**Data source:**  
`nyc_taxi.gold_location_pair_stats`

**Planned analysis:**

- filter short trips;
- focus on nearby zone pairs;
- identify high-volume short routes;
- estimate potential grouped-ride candidates.

**Key findings:**  
_To be filled after running analytical SQL queries._

**Business interpretation:**  
_To be filled after reviewing results._

---

## Business Recommendations

### Recommendation 1: Zone-Based Dynamic Pricing

**Hypothesis:**  
If demand is highly concentrated in specific zones, pricing can be adjusted based on pickup/dropoff zones and demand intensity.

**Data support:**  
_To be filled based on top pickup/dropoff zones._

**Expected impact:**  
Better demand balancing and improved revenue management.

---

### Recommendation 2: Peak-Hour Dynamic Pricing

**Hypothesis:**  
If demand is significantly higher during specific hours, dynamic pricing can help capture peak demand and improve profitability.

**Data support:**  
_To be filled based on peak hour analysis._

**Expected impact:**  
Higher revenue during high-demand periods and better driver allocation.

---

### Recommendation 3: Promotions for Short Trips in High-Demand Areas

**Hypothesis:**  
If short trips dominate in specific high-activity zones, targeted promotions can increase trip volume and customer retention.

**Data support:**  
_To be filled based on short trip zones and routes._

**Expected impact:**  
More frequent usage for short urban trips and stronger customer engagement in dense areas.

---

### Recommendation 4: Ensure 24/7 Card Payment Reliability

**Hypothesis:**  
If card payments dominate and cash usage declines, payment processing reliability becomes business-critical.

**Data support:**  
_To be filled based on payment trend analysis._

**Expected impact:**  
Reduced payment failures and better customer experience.

---

### Recommendation 5: Grouped Rides for Short Nearby Trips

**Hypothesis:**  
High-volume short trips between nearby zones can be partially grouped into shared rides.

**Data support:**  
_To be filled based on ridesharing opportunity analysis._

**Expected impact:**

- lower passenger prices;
- better vehicle utilization;
- reduced congestion;
- potential environmental benefits.

---

## Economic Impact Model for Grouped Short Trips

The simplified economic model uses the following assumptions:

- 5% of eligible short nearby trips can be grouped;
- 30% of passengers accept grouped rides;
- a $5 discount is provided for grouped rides;
- a $2 privacy fee is charged when passengers keep an individual ride.

**Formula overview:**

```text
eligible_short_trips = short nearby trips

groupable_trips = eligible_short_trips * 5%

accepted_group_trips = groupable_trips * 30%

declined_group_trips = groupable_trips * 70%

discount_cost = accepted_group_trips * $5

privacy_fee_revenue = declined_group_trips * $2

estimated_net_effect = privacy_fee_revenue - discount_cost
```

## Model result:

To be filled after running the economic impact SQL query.

## Interpretation:
To be filled after reviewing the result.

## Limitations
- The dataset does not contain direct passenger intent or trip purpose.
- Passenger motives are inferred from time, location, trip distance, and demand patterns.
- The ridesharing model is simplified and does not include operational costs, driver compensation, matching constraints, or route detours.
- The economic impact estimate should be treated as a directional business hypothesis, not a final financial forecast.

## Related SQL

Analytical queries are stored in:
```
sql/analytics_questions.sql
```

