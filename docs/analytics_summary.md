# NYC Taxi Analytical Summary

This document summarizes the main business and analytical findings for the NYC Yellow Taxi data engineering project.

The analysis is based on the full year 2024 and uses validated ClickHouse Gold marts produced by the Spark pipeline.

The current data platform contains a broader historical dataset covering:

```text
2016-01 → 2026-01
```

However, this analytical summary intentionally focuses on the 2024 calendar year as a representative business-analysis snapshot. The 2024 scope keeps the analysis comparable across months, avoids partial-year interpretation issues, and provides a clean baseline for demand, revenue, payment, route, and grouped-ride opportunity analysis.

At the current stage, the analytical SQL queries in this document use the original ClickHouse Gold marts directly. The project also includes a downstream dbt analytics layer, but Superset dashboards and the SQL analysis in this document have not yet been migrated to dbt marts.

## Executive Summary

The analysis shows five main business patterns:

1. **Demand is highly concentrated in Manhattan and airport-related flows.**
   JFK Airport, LaGuardia Airport, Midtown, Upper East Side, Times Square, Penn Station, and nearby Manhattan zones are the most important demand areas.

2. **Taxi demand has clear peak hours.**
   The strongest overall demand is concentrated in the afternoon and evening, especially from `16:00` to `19:00`, with extended high demand from `14:00` to `22:00`.

3. **Trip types behave differently.**
   Short trips dominate by volume, medium trips generate the highest total revenue, and long trips have the highest average check.

4. **Card payments dominate across trip types and across the year.**
   Credit card is the main payment method in every month of 2024. Cash remains meaningful but secondary, while the `Other` category requires monitoring.

5. **Grouped rides may be promising in dense Manhattan short-trip corridors, but the direct fare model is slightly negative.**
   A grouped ride pilot should be evaluated as an operational efficiency and sustainability initiative, not only as a direct fare uplift mechanism.

## Methodology Notes

The analysis uses aggregated ClickHouse Gold marts:

```text
nyc_taxi.gold_daily_trips
nyc_taxi.gold_hourly_trips
nyc_taxi.gold_location_pair_stats
nyc_taxi.gold_payment_type_stats
```

These marts are produced by the Spark pipeline and validated through the project’s Silver, Gold Object Storage, and ClickHouse quality gates before being used for analytics.

Because Gold marts are already aggregated, metrics are calculated using summed values:

```text
trip volume = sum(trips_count)
total revenue = sum(total_revenue)
total tips = sum(total_tips)
```

Average metrics are calculated as weighted averages, for example:

```text
avg_check = sum(total_revenue) / sum(trips_count)

avg_trip_distance =
sum(avg_trip_distance * trips_count) / sum(trips_count)

avg_trip_duration =
sum(avg_trip_duration_minutes * trips_count) / sum(trips_count)
```

This avoids using simple averages such as `avg(avg_check)`, which would incorrectly give the same weight to low-volume and high-volume groups.

Trip type is derived in the Silver layer using trip distance:

```text
short   — trip_distance < 2 miles
medium  — 2 <= trip_distance <= 10 miles
long    — trip_distance > 10 miles
```

Passenger motivation is not directly available in the dataset. It is inferred from trip type, time, zones, routes, payment behavior, and airport-related flows.

The project now also includes a dbt analytics layer in:

```text
dbt/
```

The dbt layer reads validated ClickHouse Gold tables as sources and builds downstream analytical models in:

```text
nyc_taxi_analytics_dbt
```

This document, however, keeps the original 2024 business analysis based on the ClickHouse Gold marts and SQL queries stored in:

```text
sql/analytics/
```


## Analytical Findings

### 1. Pickup and Dropoff Demand Concentration

**Question:**
Which NYC Taxi zones have the highest number of pickups and dropoffs?

**Related SQL files:**

```text
sql/analytics/01_top_pickup_zones.sql
sql/analytics/02_top_dropoff_zones.sql
```

**Data source:**
`nyc_taxi.gold_location_pair_stats`

#### Top Pickup Zones

| Rank | Borough | Pickup Zone | Trips Count | Total Revenue | Avg Check |
|---:|---|---|---:|---:|---:|
| 1 | Queens | JFK Airport | 1,862,508 | 154,046,384.58 | 82.71 |
| 2 | Manhattan | Upper East Side South | 1,851,550 | 38,747,414.70 | 20.93 |
| 3 | Manhattan | Midtown Center | 1,843,101 | 46,562,529.60 | 25.26 |
| 4 | Manhattan | Upper East Side North | 1,672,581 | 35,347,966.50 | 21.13 |
| 5 | Manhattan | Midtown East | 1,367,831 | 33,643,354.06 | 24.60 |
| 6 | Manhattan | Times Sq/Theatre District | 1,325,602 | 38,452,248.78 | 29.01 |
| 7 | Manhattan | Penn Station/Madison Sq West | 1,309,187 | 33,516,774.03 | 25.60 |
| 8 | Manhattan | Lincoln Square East | 1,267,019 | 28,334,847.52 | 22.36 |
| 9 | Queens | LaGuardia Airport | 1,252,742 | 85,870,715.61 | 68.55 |
| 10 | Manhattan | Murray Hill | 1,129,177 | 27,505,937.50 | 24.36 |

#### Top Dropoff Zones

| Rank | Borough | Dropoff Zone | Trips Count | Total Revenue | Avg Check |
|---:|---|---|---:|---:|---:|
| 1 | Manhattan | Upper East Side North | 1,737,116 | 38,022,857.14 | 21.89 |
| 2 | Manhattan | Upper East Side South | 1,664,934 | 34,889,416.72 | 20.96 |
| 3 | Manhattan | Midtown Center | 1,477,330 | 37,251,601.50 | 25.22 |
| 4 | Manhattan | Times Sq/Theatre District | 1,244,237 | 39,899,694.12 | 32.07 |
| 5 | Manhattan | Murray Hill | 1,165,919 | 28,328,822.04 | 24.30 |
| 6 | Manhattan | Midtown East | 1,137,399 | 27,918,219.60 | 24.55 |
| 7 | Manhattan | Lincoln Square East | 1,099,426 | 25,591,948.66 | 23.28 |
| 8 | Manhattan | Upper West Side South | 1,092,811 | 26,910,187.40 | 24.62 |
| 9 | Manhattan | East Chelsea | 1,029,723 | 25,674,766.34 | 24.93 |
| 10 | Manhattan | Lenox Hill West | 1,025,502 | 23,267,357.08 | 22.69 |

**Key finding:**
Pickup demand is concentrated in two groups: airports and dense Manhattan zones. JFK Airport and LaGuardia Airport combine high trip volume with much higher average checks than most Manhattan zones. Dropoff demand is concentrated almost entirely in Manhattan.

**Business implication:**
Airport flows and dense Manhattan demand should be managed separately. Airports are high-value pickup zones, while Manhattan zones require continuous demand monitoring, driver allocation, and peak-hour supply planning.

---

### 2. Peak Taxi Demand Hours

**Question:**
What hours have the highest taxi demand?

**Related SQL file:**

```text
sql/analytics/03_peak_hours.sql
```

**Data source:**
`nyc_taxi.gold_hourly_trips`

#### Top Demand Hours

| Rank | Pickup Hour | Trips Count | Trips Share, % | Total Revenue | Avg Check | Avg Trip Distance | Avg Trip Duration, min |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 18 | 2,798,610 | 7.12 | 79,528,821.08 | 28.42 | 3.01 | 17.24 |
| 2 | 17 | 2,667,442 | 6.78 | 79,846,312.73 | 29.93 | 3.18 | 19.23 |
| 3 | 19 | 2,471,756 | 6.29 | 70,451,247.56 | 28.50 | 3.20 | 16.32 |
| 4 | 16 | 2,433,870 | 6.19 | 77,693,572.52 | 31.92 | 3.53 | 20.78 |
| 5 | 15 | 2,408,194 | 6.12 | 71,965,355.30 | 29.88 | 3.51 | 20.83 |
| 6 | 14 | 2,330,893 | 5.93 | 69,729,121.32 | 29.92 | 3.56 | 20.32 |
| 7 | 21 | 2,313,754 | 5.88 | 64,683,031.84 | 27.96 | 3.46 | 15.70 |
| 8 | 20 | 2,266,228 | 5.76 | 63,391,014.30 | 27.97 | 3.41 | 15.88 |
| 9 | 13 | 2,172,682 | 5.52 | 62,799,837.26 | 28.90 | 3.41 | 19.22 |
| 10 | 22 | 2,139,338 | 5.44 | 61,037,470.48 | 28.53 | 3.61 | 15.97 |

**Key finding:**
Taxi demand is strongest in the afternoon and evening. The main demand peak is concentrated around:

```text
16:00–19:00
```

Demand also remains strong from:

```text
14:00–22:00
```

The highest revenue hours are also concentrated in the same period:

```text
17:00 — 79,846,312.73
18:00 — 79,528,821.08
16:00 — 77,693,572.52
15:00 — 71,965,355.30
19:00 — 70,451,247.56
```

**Business implication:**
The afternoon and evening peak is both high-volume and high-revenue. This period should be prioritized for driver incentives, dynamic pricing, and operational monitoring.

---

### 3. Trip Type Distribution

**Question:**
How are taxi trips distributed by trip type?

**Related SQL file:**

```text
sql/analytics/04_trip_type_distribution.sql
```

**Data source:**
`nyc_taxi.gold_hourly_trips`

#### Trip Type Distribution

| Trip Type | Trips Count | Trips Share, % | Total Revenue | Avg Check | Avg Trip Distance | Avg Trip Duration, min | Avg Cost per Mile |
|---|---:|---:|---:|---:|---:|---:|---:|
| short | 21,554,258 | 54.81 | 375,222,555.15 | 17.41 | 1.13 | 10.16 | 15.35 |
| medium | 14,543,115 | 36.98 | 472,680,345.51 | 32.50 | 4.03 | 22.06 | 8.06 |
| long | 3,230,102 | 8.21 | 279,701,200.50 | 86.59 | 16.03 | 47.63 | 5.40 |

**Key finding:**
Trip types represent three different business segments:

```text
short trips  → largest volume, highest cost per mile
medium trips → highest total revenue
long trips   → highest average check
```

Short trips represent more than half of all trips, but medium trips generate the highest total revenue. Long trips are much less frequent but are high-value rides.

**Business implication:**
Short trips are important for urban mobility and product experimentation. Medium trips are the core revenue segment. Long trips should be monitored as a high-value segment, likely connected to airport and inter-borough flows.

---

### 4. Peak Hours by Trip Type

**Question:**
What are the peak hours for short, medium, and long trips?

**Related SQL file:**

```text
sql/analytics/05_peak_hours_by_trip_type.sql
```

**Data source:**
`nyc_taxi.gold_hourly_trips`

#### Peak Hours Summary

| Trip Type | Main Peak Window | Strongest Hour | Trips at Strongest Hour | Business Pattern |
|---|---|---:|---:|---|
| short | 15:00–19:00 | 18 | 1,667,092 | Dense local urban movement |
| medium | 17:00–22:00 | 21 | 995,816 | Evening cross-neighborhood movement |
| long | 13:00–17:00 | 16 | 235,137 | Airport / long-distance flows |

#### Top 5 Hours by Trip Type

| Trip Type | Rank | Pickup Hour | Trips Count | Share within Type, % | Avg Check | Avg Duration, min |
|---|---:|---:|---:|---:|---:|---:|
| short | 1 | 18 | 1,667,092 | 7.73 | 18.88 | 10.51 |
| short | 2 | 17 | 1,566,369 | 7.27 | 19.25 | 11.11 |
| short | 3 | 19 | 1,415,055 | 6.57 | 18.34 | 9.86 |
| short | 4 | 16 | 1,394,104 | 6.47 | 19.12 | 11.07 |
| short | 5 | 15 | 1,388,723 | 6.44 | 17.29 | 11.22 |
| medium | 1 | 21 | 995,816 | 6.85 | 31.20 | 19.70 |
| medium | 2 | 22 | 970,297 | 6.67 | 31.26 | 19.62 |
| medium | 3 | 18 | 955,530 | 6.57 | 33.59 | 22.90 |
| medium | 4 | 17 | 903,956 | 6.22 | 34.81 | 24.95 |
| medium | 5 | 20 | 903,385 | 6.21 | 31.57 | 20.19 |
| long | 1 | 16 | 235,137 | 7.28 | 93.05 | 60.81 |
| long | 2 | 15 | 231,397 | 7.16 | 89.97 | 60.44 |
| long | 3 | 14 | 230,283 | 7.13 | 88.91 | 56.47 |
| long | 4 | 17 | 197,117 | 6.10 | 92.48 | 57.54 |
| long | 5 | 13 | 195,734 | 6.06 | 87.29 | 51.87 |

**Key finding:**
Different trip types peak at different times. Short trips follow the general evening peak, medium trips peak later in the evening, and long trips peak earlier in the afternoon.

**Business implication:**
Operational planning should not use one generic peak-hour strategy. Driver allocation, pricing, and airport-flow management should differ by trip type and time of day.

---

### 5. Top Pickup and Dropoff Zones by Trip Type

**Question:**
What are the top pickup and dropoff zones for each trip type?

**Related SQL file:**

```text
sql/analytics/06_top_zones_by_trip_type.sql
```

**Data source:**
`nyc_taxi.gold_location_pair_stats`

#### Top Zones by Trip Type

| Zone Role | Trip Type | Rank | Borough | Zone | Trips Count | Avg Check | Avg Trip Distance |
|---|---|---:|---|---|---:|---:|---:|
| pickup | short | 1 | Manhattan | Upper East Side South | 1,400,320 | 16.76 | 1.07 |
| pickup | short | 2 | Manhattan | Midtown Center | 1,171,778 | 18.51 | 1.13 |
| pickup | short | 3 | Manhattan | Upper East Side North | 1,163,541 | 16.40 | 1.12 |
| pickup | medium | 1 | Queens | LaGuardia Airport | 751,517 | 61.39 | 8.17 |
| pickup | medium | 2 | Manhattan | Midtown Center | 600,347 | 30.76 | 3.44 |
| pickup | medium | 3 | Manhattan | Upper East Side North | 493,545 | 30.18 | 3.52 |
| pickup | long | 1 | Queens | JFK Airport | 1,544,469 | 92.07 | 18.03 |
| pickup | long | 2 | Queens | LaGuardia Airport | 484,169 | 80.96 | 12.37 |
| pickup | long | 3 | Manhattan | Times Sq/Theatre District | 114,457 | 90.46 | 14.93 |
| dropoff | short | 1 | Manhattan | Upper East Side South | 1,294,158 | 16.78 | 1.05 |
| dropoff | short | 2 | Manhattan | Upper East Side North | 1,170,873 | 16.17 | 1.11 |
| dropoff | short | 3 | Manhattan | Midtown Center | 1,030,602 | 18.63 | 1.10 |
| dropoff | medium | 1 | Manhattan | Upper East Side North | 539,485 | 30.77 | 3.49 |
| dropoff | medium | 2 | Manhattan | Upper West Side South | 396,250 | 31.68 | 3.48 |
| dropoff | medium | 3 | Manhattan | Midtown Center | 378,985 | 31.85 | 3.44 |
| dropoff | long | 1 | Queens | JFK Airport | 352,547 | 91.14 | 17.56 |
| dropoff | long | 2 | Queens | LaGuardia Airport | 255,591 | 74.27 | 11.62 |
| dropoff | long | 3 | Manhattan | Times Sq/Theatre District | 139,577 | 90.36 | 15.76 |

**Key finding:**
Trip types have different spatial patterns:

```text
short trips  → dense Manhattan local zones
medium trips → LaGuardia + Manhattan destinations
long trips   → JFK, LaGuardia, and Times Square
```

**Business implication:**
`trip_type` is useful not only as a distance category but also as an operational segmentation dimension. Airport flows, dense Manhattan movement, and high-value long-distance trips should be managed differently.

---

### 6. Payment Methods by Trip Type

**Question:**
How do payment methods differ across trip types?

**Related SQL file:**

```text
sql/analytics/07_payment_methods_by_trip_type.sql
```

**Data source:**
`nyc_taxi.gold_payment_type_stats`

#### Payment Method Summary by Trip Type

| Trip Type | Credit Card Share, % | Cash Share, % | Other Share, % | Credit Card Avg Check | Credit Card Avg Tip | Credit Card Tips Share, % |
|---|---:|---:|---:|---:|---:|---:|
| short | 77.44 | 14.44 | 6.61 | 17.92 | 2.73 | 15.23 |
| medium | 72.99 | 11.28 | 14.69 | 33.97 | 4.97 | 14.64 |
| long | 79.48 | 14.48 | 4.16 | 89.30 | 12.41 | 13.90 |

**Key finding:**
Credit card is the dominant payment method across all trip types. Long trips have the highest credit card share and the highest card average check. Medium trips have an unusually visible `Other` payment category.

Cash tips appear close to zero in the data, but this likely reflects how tips are recorded rather than actual passenger behavior.

**Business implication:**
Card payment infrastructure is business-critical. The `Other` payment category should be monitored separately, especially for medium trips.

---

### 7. Payment Preference Trends Over Time

**Question:**
How did payment preferences evolve over time during 2024?

**Related SQL file:**

```text
sql/analytics/08_payment_preference_trends.sql
```

**Data source:**
`nyc_taxi.gold_payment_type_stats`

#### Monthly Payment Method Share

| Month | Credit Card Share, % | Cash Share, % | Other Share, % | Dispute Share, % | No Charge Share, % |
|---|---:|---:|---:|---:|---:|
| 2024-01 | 80.08 | 14.72 | 4.06 | 0.80 | 0.35 |
| 2024-02 | 80.00 | 13.65 | 5.19 | 0.81 | 0.34 |
| 2024-03 | 74.80 | 13.27 | 10.74 | 0.84 | 0.36 |
| 2024-04 | 74.18 | 13.15 | 11.51 | 0.82 | 0.34 |
| 2024-05 | 74.70 | 13.26 | 10.81 | 0.87 | 0.35 |
| 2024-06 | 74.25 | 13.05 | 11.41 | 0.92 | 0.37 |
| 2024-07 | 75.00 | 14.55 | 8.93 | 1.09 | 0.44 |
| 2024-08 | 75.07 | 14.88 | 8.39 | 1.20 | 0.46 |
| 2024-09 | 74.01 | 12.10 | 12.46 | 1.04 | 0.38 |
| 2024-10 | 76.76 | 12.29 | 9.47 | 1.08 | 0.39 |
| 2024-11 | 76.79 | 12.10 | 9.65 | 1.06 | 0.40 |
| 2024-12 | 76.93 | 13.14 | 8.29 | 1.21 | 0.43 |

**Key finding:**
Credit card remained the dominant payment method in every month of 2024.

```text
Credit card share range: 74.01%–80.08%
Cash share range:        12.10%–14.88%
Other share range:        4.06%–12.46%
```

The trend is not a simple cash decline. A more accurate interpretation is:

```text
card payments dominate consistently;
cash remains relevant but secondary;
Other payment category increased significantly in several months;
card share recovered in the last quarter of the year.
```

**Business implication:**
Payment monitoring should focus on card reliability and abnormal changes in the `Other` category. A sudden drop in card share or spike in `Other` share may indicate a payment processing, mapping, or data quality issue.

---

### 8. Short Trip Ridesharing Opportunities

**Question:**
Is there an opportunity to organize grouped rides for short trips in nearby zones?

**Related SQL file:**

```text
sql/analytics/09_short_trip_ridesharing_opportunities.sql
```

**Data source:**
`nyc_taxi.gold_location_pair_stats`

#### Top Short-Trip Ridesharing Candidates

| Rank | Pickup Zone | Dropoff Zone | Trips Count | Avg Check | Avg Distance | Avg Duration, min | Potential Groupable Trips | Direct Fare Impact |
|---:|---|---|---:|---:|---:|---:|---:|---:|
| 1 | Upper East Side South | Upper East Side North | 270,199 | 15.65 | 1.04 | 7.72 | 13,510 | -1,350.99 |
| 2 | Upper East Side North | Upper East Side South | 233,217 | 16.08 | 1.03 | 8.63 | 11,661 | -1,166.09 |
| 3 | Upper East Side South | Upper East Side South | 179,937 | 13.77 | 0.61 | 6.15 | 8,997 | -899.68 |
| 4 | Upper East Side North | Upper East Side North | 175,571 | 12.92 | 0.59 | 5.00 | 8,779 | -877.85 |
| 5 | Midtown Center | Upper East Side South | 126,333 | 17.33 | 1.04 | 10.09 | 6,317 | -631.67 |
| 6 | Upper East Side South | Midtown Center | 111,979 | 17.52 | 1.02 | 10.91 | 5,599 | -559.90 |
| 7 | Upper East Side South | Midtown East | 103,577 | 16.51 | 0.97 | 9.46 | 5,179 | -517.88 |
| 8 | Upper West Side South | Lincoln Square East | 101,987 | 14.54 | 0.87 | 6.73 | 5,099 | -509.94 |
| 9 | Lincoln Square East | Upper West Side South | 101,402 | 14.92 | 0.98 | 6.86 | 5,070 | -507.01 |
| 10 | Upper West Side South | Upper West Side North | 96,424 | 13.81 | 0.83 | 5.58 | 4,821 | -482.12 |

**Key finding:**
The strongest grouped ride candidates are short, frequent, same-borough Manhattan routes. Most candidates are concentrated around Upper East Side, Midtown, Upper West Side, Lincoln Square, Penn Station, and Times Square.

**Business implication:**
Grouped rides should not be launched broadly. A controlled pilot is more appropriate, focused on dense Manhattan corridors with high short-trip volume.

## Business Recommendations

### Recommendation 1: Zone-Based Dynamic Pricing

**Hypothesis:**
If demand is highly concentrated in specific zones, pricing and driver incentives can be adjusted based on pickup/dropoff zones and demand intensity.

**Data support:**

Airport pickup zones combine high trip volume with high average checks:

```text
JFK Airport:       1,862,508 trips, avg_check 82.71
LaGuardia Airport: 1,252,742 trips, avg_check 68.55
```

Dense Manhattan zones also generate very high recurring demand:

```text
Upper East Side South
Upper East Side North
Midtown Center
Times Sq/Theatre District
Penn Station/Madison Sq West
```

**Recommended actions:**

- monitor airport zones separately from dense Manhattan zones;
- create zone-level demand dashboards;
- test zone-based driver incentives in high-demand pickup areas;
- use different pricing logic for airport flows and dense urban trips;
- combine pickup/dropoff concentration with peak-hour analysis.

**Expected impact:**

- better driver positioning;
- better demand balancing;
- improved revenue management;
- better operational control during demand spikes.

---

### Recommendation 2: Peak-Hour Dynamic Pricing

**Hypothesis:**
If demand is significantly higher during specific hours, dynamic pricing and driver incentives can help capture peak demand and improve service availability.

**Data support:**

The main demand window is:

```text
14:00–22:00
```

The strongest peak is:

```text
16:00–19:00
```

Peak patterns differ by trip type:

```text
short trips  → 15:00–19:00
medium trips → 17:00–22:00
long trips   → 13:00–17:00
```

**Recommended actions:**

- apply dynamic pricing or driver incentives during `16:00–19:00`;
- monitor extended evening demand from `14:00` to `22:00`;
- manage long-trip and airport-related demand earlier, from `13:00` to `17:00`;
- combine peak-hour demand with zone-level demand to identify the most important supply areas.

**Expected impact:**

- higher revenue during high-demand periods;
- better driver allocation;
- reduced passenger waiting time;
- stronger operational planning for different trip types.

---

### Recommendation 3: Short-Trip Promotions in High-Demand Areas

**Hypothesis:**
If short trips dominate in dense high-activity zones, targeted promotions can increase trip volume and improve customer retention.

**Data support:**

Short trips represent the majority of trips:

```text
short trips: 21,554,258 trips
trip share:  54.81%
```

They also have the highest cost per mile:

```text
avg_cost_per_mile: 15.35
```

Top short-trip zones are concentrated in Manhattan:

```text
Upper East Side South
Upper East Side North
Midtown Center
```

**Recommended actions:**

- test short-trip promotions in dense Manhattan zones;
- evaluate whether high cost per mile reduces willingness to use taxis for short trips;
- combine promotions with short-trip peak windows;
- monitor whether promotions increase volume without reducing revenue too strongly.

**Expected impact:**

- more frequent short-distance usage;
- improved taxi competitiveness for short urban trips;
- stronger customer engagement in dense areas;
- foundation for grouped ride experiments.

---

### Recommendation 4: Ensure 24/7 Card Payment Reliability

**Hypothesis:**
If card payments dominate taxi trips, payment processing reliability becomes business-critical.

**Data support:**

Credit card dominates across all trip types:

```text
short trips credit card share:  77.44%
medium trips credit card share: 72.99%
long trips credit card share:   79.48%
```

Credit card also dominates every month:

```text
monthly card share range: 74.01%–80.08%
```

The `Other` category increased significantly in several months:

```text
2024-03 — 10.74%
2024-04 — 11.51%
2024-06 — 11.41%
2024-09 — 12.46%
```

**Recommended actions:**

- monitor card payment processing 24/7;
- create alerts for abnormal drops in card share;
- investigate spikes in the `Other` payment category;
- continue supporting cash because it remains around 12%–15% of trips;
- interpret cash tips carefully because they may not be fully captured.

**Expected impact:**

- reduced payment failures;
- better customer experience;
- protected revenue from card-heavy trips;
- earlier detection of payment or data mapping issues.

---

### Recommendation 5: Controlled Grouped Rides Pilot

**Hypothesis:**
High-volume short trips between nearby zones can be partially grouped into shared rides.

**Data support:**

Top candidate routes are concentrated in dense Manhattan areas:

```text
Upper East Side South → Upper East Side North
Upper East Side North → Upper East Side South
Upper West Side South → Lincoln Square East
Penn Station/Madison Sq West → Times Sq/Theatre District
Midtown Center → Midtown South
```

These routes have high volume, short distance, and short duration.

**Recommended actions:**

- do not launch grouped rides broadly across all short trips;
- start with a controlled pilot in dense Manhattan corridors;
- use time-window matching, for example trips requested within 3–5 minutes;
- use route-overlap logic, not only taxi zone pairs;
- track acceptance rate, waiting time, detour time, driver earnings, completed grouped rides, and customer satisfaction.

**Expected impact:**

- lower passenger prices;
- better vehicle utilization;
- reduced congestion;
- potential environmental benefits;
- stronger short-trip product strategy.

## Economic Impact Model for Grouped Short Trips

The grouped ride model is a simplified directional business model for short, high-volume, same-borough routes.

The model is not a full profit calculation. It separates three different concepts:

```text
1. direct fare impact;
2. revenue per vehicle trip;
3. full profit impact.
```

Only the first two can be estimated from the 2024 analytical snapshot. Full profit impact requires additional operational cost data.

### Model Assumptions

The simplified model uses the following assumptions:

```text
5% of eligible short nearby passenger trips can be grouped
30% of groupable passengers accept grouped rides
70% of groupable passengers keep individual rides
grouped ride discount = 5 USD per accepting passenger
individual privacy/time fee = 2 USD per declining passenger
2 accepting passengers form 1 grouped vehicle trip
```

### Direct Fare Impact

Direct fare impact compares the additional privacy fee revenue with the discount cost.

Formula:

```text
groupable_passenger_trips = eligible_short_trips * 5%

grouped_accepting_passenger_trips = groupable_passenger_trips * 30%

individual_privacy_passenger_trips = groupable_passenger_trips * 70%

grouped_discount_cost = grouped_accepting_passenger_trips * 5

privacy_fee_revenue = individual_privacy_passenger_trips * 2

estimated_direct_fare_impact = privacy_fee_revenue - grouped_discount_cost
```

Under the current assumptions, the direct fare impact is slightly negative.

Expected direct impact per groupable passenger trip:

```text
30% accepting grouped rides * 5 USD discount = 1.50 USD expected discount

70% choosing individual rides * 2 USD fee = 1.40 USD expected additional fee

net direct impact = -0.10 USD per groupable passenger trip
```

This means that the discount cost is slightly higher than the additional privacy fee revenue.

### Revenue per Vehicle Trip

Direct fare impact is not the only useful metric. Grouped rides can reduce the number of vehicle trips needed to serve the same passenger demand.

Example:

```text
Before grouping:
Passenger A pays 15 USD
Passenger B pays 15 USD
Total revenue = 30 USD
Vehicle trips = 2
Revenue per vehicle trip = 15 USD

After grouping:
Passenger A pays 10 USD after 5 USD discount
Passenger B pays 10 USD after 5 USD discount
Total revenue = 20 USD
Vehicle trips = 1
Revenue per vehicle trip = 20 USD
```

In this example:

```text
revenue per passenger decreases from 15 USD to 10 USD

but

revenue per vehicle trip increases from 15 USD to 20 USD
```

Formula:

```text
baseline_revenue_per_vehicle_trip = avg_check

grouped_revenue_per_vehicle_trip =
2 * (avg_check - grouped_ride_discount)

vehicle_trip_revenue_uplift =
grouped_revenue_per_vehicle_trip - baseline_revenue_per_vehicle_trip
```

Simplified:

```text
vehicle_trip_revenue_uplift = avg_check - 10
```

This metric shows how much more revenue one vehicle trip can generate when two passengers are grouped into one ride with a 5 USD discount each.

It should not be interpreted as profit. It is a revenue efficiency metric.

### Model Result for the Top Route

```text
Route:
Upper East Side South → Upper East Side North

trips_count: 270,199
avg_check: 15.65
potential_groupable_passenger_trips: 13,510
grouped_accepting_passenger_trips: 4,053
grouped_vehicle_trips: 2,026
individual_privacy_passenger_trips: 9,457

baseline_revenue_per_vehicle_trip: 15.65
grouped_revenue_per_vehicle_trip: 21.30
vehicle_trip_revenue_uplift: 5.65

grouped_discount_cost: 20,264.92
privacy_fee_revenue: 18,913.93
estimated_direct_fare_impact: -1,350.99
```

### Interpretation

The model gives two complementary signals.

First, the direct fare impact is slightly negative:

```text
estimated_direct_fare_impact < 0
```

This happens because the expected discount cost is slightly higher than expected privacy fee revenue.

Second, revenue per vehicle trip can increase:

```text
vehicle_trip_revenue_uplift > 0
```

This happens because one vehicle trip serves two passengers instead of one.

The business case becomes positive only if operational savings from avoiding separate vehicle trips exceed the remaining fare gap and additional grouped-ride costs.

Full profit logic:

```text
full_profit_impact =
direct_fare_impact
+ operational_savings_from_avoided_vehicle_trips
- additional_matching_costs
- additional_detour_costs
- additional_waiting_time_costs
```

The 2024 analytical snapshot does not contain the operational cost data required to calculate full profit impact.

### Business Interpretation

Grouped rides should not be evaluated only as a direct fare uplift mechanism.

A more accurate interpretation is:

```text
direct fare model:
slightly negative under current assumptions

vehicle revenue efficiency:
potentially positive for dense short-trip corridors

full business case:
depends on operational savings, matching quality, detour time, driver economics, and passenger acceptance
```

Grouped rides should therefore be tested as a controlled operational efficiency and sustainability pilot, not launched broadly.

The best pilot candidates are high-volume, short-distance Manhattan routes where matching probability is high and detour risk is limited.

## Overall Strategic Conclusions

The analysis supports the following strategic priorities:

```text
1. Zone-based demand management for airports and dense Manhattan areas.
2. Peak-hour pricing and driver incentives during high-demand windows.
3. Short-trip product experimentation in dense Manhattan zones.
4. Card payment reliability and payment-category monitoring.
5. Controlled grouped ride pilot for high-volume short Manhattan corridors.
```

## Limitations and Additional Data Needs

The dataset does not contain direct passenger intent or trip purpose. Passenger motives are inferred from time, location, trip distance, payment behavior, and demand patterns.

This document focuses on the 2024 calendar year. The current data platform contains a broader historical range, but business patterns may vary across years due to seasonality, market changes, airport traffic, pricing rules, external events, and post-2024 data behavior. For production-grade strategy, the same analysis should be repeated across multiple years and compared against the 2024 baseline.

The analysis would be stronger with additional data sources:

```text
POI categories
land use / zoning data
weather data
event schedules
airport flight schedules
traffic and congestion data
public transit disruption data
tourism and hotel occupancy data
aggregated booking context
```

The ridesharing model is simplified and does not include:

```text
operational costs
driver compensation
matching constraints
route overlap
vehicle capacity
route detours
real-time driver availability
customer satisfaction
```

The economic impact estimate should be treated as a directional business hypothesis, not a final financial forecast.

Cash tips may not be fully captured in the recorded `tip_amount` field.

The `Other` payment category requires additional investigation before being interpreted as a passenger preference.

For production-grade decision-making, analytical results should be validated with more granular trip-level, operational, and external context data.

## Related SQL

Analytical queries are stored in:

```text
sql/analytics/
```

These SQL files currently query the validated ClickHouse Gold marts directly. A future improvement is to migrate selected analytical queries and Superset datasets to dbt marts where appropriate.

Main analytical query files:

```text
sql/analytics/01_top_pickup_zones.sql
sql/analytics/02_top_dropoff_zones.sql
sql/analytics/03_peak_hours.sql
sql/analytics/04_trip_type_distribution.sql
sql/analytics/05_peak_hours_by_trip_type.sql
sql/analytics/06_top_zones_by_trip_type.sql
sql/analytics/07_payment_methods_by_trip_type.sql
sql/analytics/08_payment_preference_trends.sql
sql/analytics/09_short_trip_ridesharing_opportunities.sql
```
