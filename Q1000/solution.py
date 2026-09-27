from __future__ import annotations

from typing import List, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
    DateType,
    LongType,
)


  """Create inline profile and plan lookup DataFrames and return (profiles_df, plans_df)."""
    profiles_rows = [
        ("U1", "Aarav", "Sharma", None, "P2"),
        ("U2", "Diya", "Iyer", "Diya Iyer", "P1"),
        ("U3", "Kabir", "Singh", None, "P1"),
        ("U4", "Meera", "Nair", None, "P2"),
    ]
    plans_rows = [
        ("P1", "Standard"),
        ("P2", "Premium"),
    ]

# ============================================================
# A) Core Trip Analytics
# ============================================================

def define_trip_schema() -> StructType:
    pass


def load_trips(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    pass


def parse_trip_date(df: DataFrame) -> DataFrame:
    pass


def add_trip_duration_min(df: DataFrame) -> DataFrame:
    pass


def add_cost_per_km(df: DataFrame) -> DataFrame:
    pass


def filter_peak_hour_trips(df: DataFrame) -> DataFrame:
    pass


def top_n_users_by_distance(df: DataFrame, n: int) -> DataFrame:
    pass


def avg_speed_by_zone(df: DataFrame) -> DataFrame:
    pass


def most_common_vehicle_type(df: DataFrame) -> str:
    pass


def cancellation_rate_by_zone(df: DataFrame) -> DataFrame:
    pass


def high_duration_trips(df: DataFrame, threshold_min: int) -> DataFrame:
    pass


def count_active_vehicles_by_zone(df: DataFrame) -> DataFrame:
    pass


def daily_net_revenue_trend(df: DataFrame) -> DataFrame:
    pass


def top_vehicle_by_net_revenue(df: DataFrame) -> Tuple[str, float]:
    pass


def list_zones(df: DataFrame) -> List[str]:
    pass


def trips_in_date_range(df: DataFrame, start: str, end: str) -> DataFrame:
    pass


def flag_safety_risk(
    df: DataFrame, speed_threshold: float, brake_threshold: int
) -> DataFrame:
    pass


def top_n_risky_vehicles(df: DataFrame, n: int) -> DataFrame:
    pass


def avg_fare_by_payment_method(df: DataFrame) -> DataFrame:
    pass


def get_longest_trip(df: DataFrame) -> Tuple[str, int]:
    pass


# ============================================================
# B) Inline Join + Enrichment
# ============================================================

def define_user_profile_schema() -> StructType:
    pass


def define_vehicle_schema() -> StructType:
    pass


def load_inline_profiles_and_plans(
    spark: SparkSession,
    profile_schema: StructType,
    plan_schema: StructType,
) -> Tuple[DataFrame, DataFrame]:
    pass


def join_profiles_with_plans(
    profiles_df: DataFrame, plans_df: DataFrame
) -> DataFrame:
    pass


def enrich_full_name(profile_joined_df: DataFrame) -> DataFrame:
    pass


# ============================================================
# C) Unix timestamp conversions
# ============================================================

def add_start_epoch_seconds(df: DataFrame) -> DataFrame:
    pass


def add_start_ts_from_epoch(df: DataFrame) -> DataFrame:
    pass


# ============================================================
# D) Additional analytics
# ============================================================

def revenue_share_by_vehicle_type(df: DataFrame) -> List[str]:
    pass


def busiest_zone_by_trips(df: DataFrame) -> Tuple[str, int]:
    pass


def completed_trips_by_zone(df: DataFrame) -> DataFrame:
    pass


def add_utilization_score(df: DataFrame) -> DataFrame:
    pass


def top_n_users_by_utilization(df: DataFrame, n: int) -> DataFrame:
    pass


def median_duration_by_zone(df: DataFrame) -> DataFrame:
    pass


def minmax_normalize_fare(df: DataFrame) -> DataFrame:
    pass


def detect_outlier_fares(df: DataFrame) -> DataFrame:
    pass


def monthly_net_revenue_by_zone(df: DataFrame) -> DataFrame:
    pass


def weekday_peak_trip_counts(df: DataFrame) -> DataFrame:
    pass


def pivot_payment_counts_by_zone(df: DataFrame) -> DataFrame:
    pass


def calculate_net_revenue(df: DataFrame) -> DataFrame:
    pass


def flag_service_due(df: DataFrame, due_days: int) -> DataFrame:
    pass
