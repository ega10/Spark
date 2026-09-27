# SmartCity Mobility Mega Assessment (Q1000)

## How to run
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python3 test_runner.py
```

This will:
- run the tests defined in `test_config.json`
- load your functions from `solution.py`
- print `[PASS]` / `[FAIL]` per test case
- write results to `test_report.log`

## Dataset
CSV files are provided in `data/`:

- `data/mobility_trips.csv`
- `data/vehicles.csv`
- `data/users.csv` (for your own practice; tests use inline profiles/plans too)

`mobility_trips.csv` columns:

- trip_id (string)
- user_id (string)
- vehicle_id (string)
- zone (string)
- trip_date (string `yyyy-MM-dd`)
- start_ts (string `yyyy-MM-dd HH:mm:ss`)
- end_ts (string `yyyy-MM-dd HH:mm:ss`)
- distance_km (double)
- fare (double)
- discount (double)
- payment_method (string)
- vehicle_type (string)
- speed_kmh (double)
- harsh_brake_count (int)
- status (string: Completed/Cancelled)

## Objective
Implement **40 pure PySpark functions** in `solution.py`.

### Important testing note (no dependency)
Each function is tested **independently**.
For functions that normally depend on upstream cleaning/aggregation, the tests pass a **precomputed DataFrame** fixture.

---

## Function list and LLD

### 1) define_trip_schema() -> StructType
- Return an explicit `StructType` for `mobility_trips.csv`.
- Keep all fields nullable.
- Keep `trip_date/start_ts/end_ts` as string in schema (parsed in later functions).

### 2) load_trips(spark, path, schema) -> DataFrame
- Use `spark.read.option("header", True).schema(schema).csv(path)`.

### 3) parse_trip_date(df) -> DataFrame
- Use `to_date(col("trip_date"))` and return the updated DF.

### 4) add_trip_duration_min(df) -> DataFrame
- Use `unix_timestamp(end_ts) - unix_timestamp(start_ts)` and divide by 60.
- Add `trip_duration_min` as integer minutes.

### 5) add_cost_per_km(df) -> DataFrame
- `cost_per_km = fare / distance_km` as double.

### 6) filter_peak_hour_trips(df) -> DataFrame
- Peak hours: 07–10 or 17–20 (inclusive).
- Use `hour(start_ts)` and `between`.

### 7) top_n_users_by_distance(df, n) -> DataFrame
- `groupBy("user_id").agg(sum("distance_km").alias("total_distance_km"))`
- order by total desc, tie-break user_id asc
- return top `n`.

### 8) avg_speed_by_zone(df) -> DataFrame
- `groupBy("zone").agg(avg("speed_kmh").alias("avg_speed_kmh"))`

### 9) most_common_vehicle_type(df) -> str
- `groupBy("vehicle_type").count()` then sort desc count, asc vehicle_type.
- Return the top vehicle_type string.

### 10) cancellation_rate_by_zone(df) -> DataFrame
- For each zone compute:
  - total rows
  - cancelled rows (`status == "Cancelled"`)
  - `cancellation_rate = cancelled / total`

### 11) high_duration_trips(df, threshold_min) -> DataFrame
- Filter trips with duration strictly greater than threshold.
- If input doesn’t contain `trip_duration_min`, compute it internally.

### 12) count_active_vehicles_by_zone(vehicles_df) -> DataFrame
- Filter `status == "Active"`
- `groupBy("home_zone").countDistinct("vehicle_id") as active_vehicles`
- Rename `home_zone -> zone`.

### 13) daily_net_revenue_trend(df) -> DataFrame
- `net_revenue = fare - discount`
- groupBy trip_date, sum net_revenue as `daily_net_revenue` (order by date asc)

### 14) top_vehicle_by_net_revenue(df) -> Tuple[str, float]
- Sum net revenue per vehicle_id
- return `(vehicle_id, total_net_revenue)` for top row
- if no data: return `("", 0.0)`

### 15) list_zones(df) -> List[str]
- Return sorted list of unique zones.

### 16) trips_in_date_range(df, start, end) -> DataFrame
- Inclusive filter: `trip_date` between start and end.

### 17) flag_safety_risk(df, speed_threshold, brake_threshold) -> DataFrame
- Add boolean `is_risky`:
  - `speed_kmh > speed_threshold OR harsh_brake_count > brake_threshold`

### 18) top_n_risky_vehicles(df, n) -> DataFrame
- From `is_risky == True`, count per vehicle_id as `risky_count`
- order desc, tie-break asc, limit n.

### 19) avg_fare_by_payment_method(df) -> DataFrame
- groupBy payment_method and avg fare.

### 20) get_longest_trip(df) -> Tuple[str, int]
- Longest by `trip_duration_min` (compute if missing)
- return `(trip_id, trip_duration_min)`.

### 21) define_user_profile_schema() -> StructType
- schema for inline profiles (user_id, first_name, last_name, full_name, plan_id)

### 22) define_vehicle_schema() -> StructType
- schema for inline plans (plan_id, plan_name)

### 23) load_inline_profiles_and_plans(spark, profile_schema, plan_schema) -> Tuple[DataFrame, DataFrame]
- Create inline DataFrames using `spark.createDataFrame`.

### 24) join_profiles_with_plans(profiles_df, plans_df) -> DataFrame
- Left join on plan_id; include plan_name.

### 25) enrich_full_name(df) -> DataFrame
- If full_name is null/empty: `concat_ws(" ", first_name, last_name)`

### 26) add_start_epoch_seconds(df) -> DataFrame
- Add `start_epoch_seconds = unix_timestamp(start_ts)` as long.

### 27) add_start_ts_from_epoch(df) -> DataFrame
- Add `start_ts_from_epoch = from_unixtime(start_epoch_seconds)` cast to timestamp.

### 28) revenue_share_by_vehicle_type(metrics_df) -> List[str]
- Input columns: vehicle_type, rev
- Compute total_rev per vehicle_type and share = total_rev / grand_total
- return vehicle types ordered by share desc.

### 29) busiest_zone_by_trips(df) -> Tuple[str, int]
- Zone with highest trip count (tie-break zone asc).

### 30) completed_trips_by_zone(df) -> DataFrame
- Filter status == Completed; count per zone as completed_trips; order desc.

### 31) add_utilization_score(df) -> DataFrame
- utilization_score = distance_km * trip_duration_min (compute duration if missing)

### 32) top_n_users_by_utilization(df, n) -> DataFrame
- Sum utilization_score per user and return top n.

### 33) median_duration_by_zone(df) -> DataFrame
- Use `percentile_approx(trip_duration_min, 0.5)` per zone.

### 34) minmax_normalize_fare(df) -> DataFrame
- Add fare_norm = (fare - min) / (max - min)
- If max == min: set fare_norm = 0.0

### 35) detect_outlier_fares(df) -> DataFrame
- Outlier if `fare > mean + 2*stddev_pop`

### 36) monthly_net_revenue_by_zone(df) -> DataFrame
- month = date_format(trip_date, "yyyy-MM")
- groupBy (month, zone) and sum net revenue.

### 37) weekday_peak_trip_counts(df) -> DataFrame
- Count peak trips by weekday label.

### 38) pivot_payment_counts_by_zone(df) -> DataFrame
- `groupBy("zone").pivot("payment_method").count()` then fill nulls with 0.

### 39) calculate_net_revenue(df) -> DataFrame
- Add net_revenue = fare - discount.

### 40) flag_service_due(vehicles_df, due_days) -> DataFrame
- `is_service_due = datediff(current_date(), last_service_date) > due_days`
