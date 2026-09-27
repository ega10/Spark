import io
import json
import ast
import inspect
import contextlib
from datetime import datetime

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql import functions as F

import solution as sol

with open("test_config.json") as f:
    config = json.load(f)

GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"

LOG_FILE = "test_report.log"
VISIBLE_COUNT = 0
HIDDEN_COUNT = 0

with open(LOG_FILE, "w", encoding="utf-8") as logf:
    logf.write(
        f"=== SmartCity Mobility Mega Assessment Test Run at "
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n"
    )


# ------------------------------------------------------------
# Utility helpers
# ------------------------------------------------------------
def _case_prefix(test_case) -> str:
    global VISIBLE_COUNT, HIDDEN_COUNT
    if bool(test_case.get("visible", True)):
        VISIBLE_COUNT += 1
        return f"Visible Test Case {VISIBLE_COUNT}"
    HIDDEN_COUNT += 1
    return f"Hidden Test Case {HIDDEN_COUNT}"


def _case_name(func_name: str, test_case) -> str:
    return test_case.get("name") or func_name


def _log(line: str):
    with open(LOG_FILE, "a", encoding="utf-8") as logf:
        logf.write(line + "\n")


def _pass(prefix: str, desc: str):
    line = f"[PASS] {prefix} Passed: {desc}"
    print(f"{GREEN}{line}{RESET}")
    _log(line)


def _fail(prefix: str, desc: str, expected: str, reason: str):
    line = f"[FAIL] {prefix} Failed: {desc}"
    print(f"{RED}{line}{RESET}")
    _log(line)
    e = f"Expected : {expected}"
    r = f"Reason   : {reason}"
    print(f"{RED}{e}{RESET}")
    print(f"{RED}{r}{RESET}")
    _log(e)
    _log(r)
    raise AssertionError(f"{e}\n{r}")


# ------------------------------------------------------------
# Static analysis checks
# ------------------------------------------------------------
def detect_unimplemented_function(func):
    """
    Flags these as 'unimplemented':
      - empty body (apart from docstring)
      - body is only: pass
      - body is only: raise NotImplementedError(...)
      - body is only: ...
    """
    source = inspect.getsource(func)
    tree = ast.parse(source)
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef))

    body = [
        n for n in fn.body
        if not (
            isinstance(n, ast.Expr)
            and isinstance(n.value, ast.Constant)
            and isinstance(n.value.value, str)
        )
    ]

    if len(body) == 0:
        return True, "Function body is empty"

    if len(body) == 1 and isinstance(body[0], ast.Pass):
        return True, "Function contains only 'pass'"

    if len(body) == 1 and isinstance(body[0], ast.Raise):
        if (
            isinstance(body[0].exc, ast.Call)
            and getattr(body[0].exc.func, "id", "") == "NotImplementedError"
        ):
            return True, "Function raises NotImplementedError"

    # NEW: detect a lone ellipsis body (def f(): ...)
    if len(body) == 1 and isinstance(body[0], ast.Expr):
        v = getattr(body[0], "value", None)
        if isinstance(v, ast.Constant) and v.value is Ellipsis:
            return True, "Function contains only '...' (ellipsis)"

    return False, ""


# ------------------------------------------------------------
# Spark API token checks (expanded to ALL 40 functions)
# NOTE: these are heuristics (string checks) to discourage non-Spark solutions.
# ------------------------------------------------------------
REQUIRED_TOKENS = {
    # existing ones
    "load_trips": ["spark.read", ".csv", "schema"],
    "parse_trip_date": ["to_date", "withColumn"],
    "add_trip_duration_min": ["unix_timestamp", "trip_duration_min"],
    "add_cost_per_km": ["withColumn", "cost_per_km"],
    "filter_peak_hour_trips": ["hour", "between"],
    "top_n_users_by_distance": ["groupBy", "sum", "orderBy", "limit"],
    "avg_speed_by_zone": ["groupBy", "avg"],
    "cancellation_rate_by_zone": ["when", "withColumn", "cancellation_rate"],
    "daily_net_revenue_trend": ["fare", "discount", "groupBy", "net_revenue"],
    "top_vehicle_by_net_revenue": ["groupBy", "sum", "orderBy", "collect"],
    "flag_safety_risk": ["withColumn", "is_risky"],
    "top_n_risky_vehicles": ["filter", "groupBy", "count"],
    "median_duration_by_zone": ["percentile_approx"],
    "pivot_payment_counts_by_zone": ["pivot"],

    # NEW: remaining functions (26)
    "define_trip_schema": ["StructType", "StructField"],
    "most_common_vehicle_type": ["groupBy", "count", "orderBy", "collect"],
    "high_duration_trips": ["filter", "trip_duration_min"],
    "count_active_vehicles_by_zone": ["filter", "countDistinct", "groupBy"],
    "list_zones": ["distinct", "collect"],
    "trips_in_date_range": ["between"],
    "avg_fare_by_payment_method": ["groupBy", "avg"],
    "get_longest_trip": ["orderBy", "trip_duration_min", "collect"],

    "define_user_profile_schema": ["StructType", "StructField"],
    "define_vehicle_schema": ["StructType", "StructField"],
    "load_inline_profiles_and_plans": ["createDataFrame"],
    "join_profiles_with_plans": ["join", "select"],
    "enrich_full_name": ["when", "concat_ws", "trim"],

    "add_start_epoch_seconds": ["unix_timestamp", "start_epoch_seconds"],
    "add_start_ts_from_epoch": ["from_unixtime", "start_ts_from_epoch"],

    "revenue_share_by_vehicle_type": ["groupBy", "sum", "withColumn", "share"],
    "busiest_zone_by_trips": ["groupBy", "count", "orderBy", "collect"],
    "completed_trips_by_zone": ["filter", "groupBy", "count"],
    "add_utilization_score": ["withColumn", "utilization_score"],
    "top_n_users_by_utilization": ["groupBy", "sum", "orderBy", "limit"],
    "minmax_normalize_fare": ["min", "max", "fare_norm"],
    "detect_outlier_fares": ["avg", "stddev_pop", "is_outlier"],
    "monthly_net_revenue_by_zone": ["date_format", "groupBy", "month"],
    "weekday_peak_trip_counts": ["date_format", "weekday", "groupBy"],
    "calculate_net_revenue": ["withColumn", "net_revenue"],
    "flag_service_due": ["datediff", "current_date", "is_service_due"],
}


def assert_source_contains(fn_name: str):
    if fn_name not in REQUIRED_TOKENS:
        return
    src = inspect.getsource(getattr(sol, fn_name))
    for tok in REQUIRED_TOKENS[fn_name]:
        if tok not in src:
            raise AssertionError(
                f"Expected Spark API token '{tok}' not found in {fn_name}"
            )


def assert_has_columns(df: DataFrame, required_cols):
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise AssertionError(f"Missing required columns: {missing}. Found: {df.columns}")


def assert_tuple_shape(fn_name: str, result):
    """
    Strengthen tuple return checks for known tuple functions in this assessment.
    """
    if fn_name == "top_vehicle_by_net_revenue":
        if not (isinstance(result, tuple) and len(result) == 2):
            raise AssertionError("Expected tuple(vehicle_id, total_net_revenue)")
        if not isinstance(result[0], str):
            raise AssertionError(f"Expected vehicle_id as str, got {type(result[0])}")
        if not isinstance(result[1], (float, int)):
            raise AssertionError(f"Expected total_net_revenue numeric, got {type(result[1])}")

    if fn_name == "get_longest_trip":
        if not (isinstance(result, tuple) and len(result) == 2):
            raise AssertionError("Expected tuple(trip_id, trip_duration_min)")
        if not isinstance(result[0], str):
            raise AssertionError(f"Expected trip_id as str, got {type(result[0])}")
        if not isinstance(result[1], int):
            raise AssertionError(f"Expected trip_duration_min as int, got {type(result[1])}")

    if fn_name == "busiest_zone_by_trips":
        if not (isinstance(result, tuple) and len(result) == 2):
            raise AssertionError("Expected tuple(zone, trip_count)")
        if not isinstance(result[0], str):
            raise AssertionError(f"Expected zone as str, got {type(result[0])}")
        if not isinstance(result[1], int):
            raise AssertionError(f"Expected trip_count as int, got {type(result[1])}")


def assert_df_min_columns(fn_name: str, df: DataFrame):
    """
    Minimal schema/column checks to avoid false passes where a DF is returned
    but with missing required outputs.
    """
    expected_cols = {
        "add_trip_duration_min": ["trip_duration_min"],
        "add_cost_per_km": ["cost_per_km"],
        "cancellation_rate_by_zone": ["zone", "cancellation_rate"],
        "count_active_vehicles_by_zone": ["zone", "active_vehicles"],
        "daily_net_revenue_trend": ["trip_date", "daily_net_revenue"],
        "avg_speed_by_zone": ["zone", "avg_speed_kmh"],
        "top_n_users_by_distance": ["user_id", "total_distance_km"],
        "flag_safety_risk": ["is_risky"],
        "top_n_risky_vehicles": ["vehicle_id", "risky_count"],
        "avg_fare_by_payment_method": ["payment_method", "avg_fare"],
        "add_start_epoch_seconds": ["start_epoch_seconds"],
        "add_start_ts_from_epoch": ["start_ts_from_epoch"],
        "median_duration_by_zone": ["zone", "median_duration_min"],
        "completed_trips_by_zone": ["zone", "completed_trips"],
        "add_utilization_score": ["utilization_score"],
        "top_n_users_by_utilization": ["user_id", "total_utilization"],
        "minmax_normalize_fare": ["fare_norm"],
        "detect_outlier_fares": ["is_outlier"],
        "monthly_net_revenue_by_zone": ["month", "zone", "monthly_net_revenue"],
        "weekday_peak_trip_counts": ["weekday", "peak_trips"],
        "pivot_payment_counts_by_zone": ["zone"],  # pivot columns vary by data
        "calculate_net_revenue": ["net_revenue"],
        "flag_service_due": ["is_service_due"],
        "join_profiles_with_plans": ["user_id", "plan_id", "plan_name"],
        "enrich_full_name": ["full_name"],
    }
    cols = expected_cols.get(fn_name)
    if cols:
        assert_has_columns(df, cols)


# ------------------------------------------------------------
# MAIN PARAMETERIZED TEST
# ------------------------------------------------------------
@pytest.mark.parametrize("test_case", config)
def test_configurable_functions(
    test_case,
    spark,
    trip_path,
    sample_trips_raw_df,
    sample_trips_typed_df,
    sample_trips_duration_df,
    sample_trips_risk_df,
    sample_trips_epoch_df,
    sample_trips_util_df,
    sample_metrics_df,
    sample_vehicles_typed_df,
):
    func_name = test_case["function"]
    expected = test_case["expected"]

    prefix = _case_prefix(test_case)
    desc = _case_name(func_name, test_case)

    if not hasattr(sol, func_name):
        _fail(prefix, desc, expected, "Function not found in solution.py")

    fn = getattr(sol, func_name)

    not_impl, reason = detect_unimplemented_function(fn)
    if not_impl:
        _fail(prefix, desc, expected, reason)

    # --------------------------------------------------------
    # Base argument map (always safe)
    # --------------------------------------------------------
    arg_map = {
        "spark": spark,
        "trip_path": trip_path,
        "trip_schema": sol.define_trip_schema(),
        "sample_trips_raw_df": sample_trips_raw_df,
        "sample_trips_typed_df": sample_trips_typed_df,
        "sample_trips_duration_df": sample_trips_duration_df,
        "sample_trips_risk_df": sample_trips_risk_df,
        "sample_trips_epoch_df": sample_trips_epoch_df,
        "sample_trips_util_df": sample_trips_util_df,
        "sample_metrics_df": sample_metrics_df,
        "sample_vehicles_typed_df": sample_vehicles_typed_df,
    }

    # --------------------------------------------------------
    # LAZY profile / plan setup (CRITICAL FIX)
    # --------------------------------------------------------
    needed_args = set(test_case.get("args", []))

    needs_profiles = any(a in needed_args for a in {
        "profile_schema",
        "plan_schema",
        "profiles_df",
        "plans_df",
        "profiles_joined_df",
    })

    if needs_profiles:
        try:
            profile_schema = sol.define_user_profile_schema()
            plan_schema = sol.define_vehicle_schema()

            profiles_df, plans_df = sol.load_inline_profiles_and_plans(
                spark, profile_schema, plan_schema
            )

            profiles_joined_df = sol.join_profiles_with_plans(
                profiles_df, plans_df
            )

            arg_map.update({
                "profile_schema": profile_schema,
                "plan_schema": plan_schema,
                "profiles_df": profiles_df,
                "plans_df": plans_df,
                "profiles_joined_df": profiles_joined_df,
            })

        except Exception as e:
            _fail(prefix, desc, expected, f"Setup failed: {str(e)}")

    args = [arg_map.get(a, a) for a in test_case.get("args", [])]

    try:
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer), contextlib.redirect_stderr(buffer):
            result = fn(*args)

        # -------------------------------
        # Type checks
        # -------------------------------
        if expected == "DataFrame" and not isinstance(result, DataFrame):
            raise AssertionError(f"Expected DataFrame but got {type(result)}")

        if expected == "StructType" and not isinstance(result, StructType):
            raise AssertionError(f"Expected StructType but got {type(result)}")

        if expected == "tuple" and not isinstance(result, tuple):
            raise AssertionError(f"Expected tuple but got {type(result)}")

        if expected == "str" and not isinstance(result, str):
            raise AssertionError(f"Expected str but got {type(result)}")

        if expected == "list_str":
            if not isinstance(result, list) or not all(isinstance(x, str) for x in result):
                raise AssertionError(f"Expected list[str] but got {result}")

        if expected == "tuple_df":
            if not (
                isinstance(result, tuple)
                and len(result) == 2
                and all(isinstance(x, DataFrame) for x in result)
            ):
                raise AssertionError("Expected tuple(DataFrame, DataFrame)")

        # -------------------------------
        # Stronger tuple checks (for known tuple-return functions)
        # -------------------------------
        if expected == "tuple":
            assert_tuple_shape(func_name, result)

        # -------------------------------
        # Minimal DF column presence checks
        # -------------------------------
        if expected == "DataFrame":
            assert_df_min_columns(func_name, result)

        # -------------------------------
        # Spark usage heuristics
        # -------------------------------
        assert_source_contains(func_name)

        _pass(prefix, desc)

    except AssertionError as e:
        _fail(prefix, desc, expected, str(e))
