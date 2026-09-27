from __future__ import annotations

from typing import List, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.functions import countDistinct
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


# ============================================================
# A) Core Trip Analytics
# ============================================================

def define_trip_schema() -> StructType:
    schema = StructType([
        StructField("trip_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("zone", StringType(), True),
        StructField("trip_date", StringType(), True),
        StructField("start_ts", StringType(), True),
        StructField("end_ts", StringType(), True),
        StructField("distance_km", DoubleType(), True),
        StructField("fare", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("payment_method", StringType(), True),
        StructField("vehicle_type", StringType(), True),
        StructField("speed_kmh", DoubleType(), True),
        StructField("harsh_brake_count", IntegerType(), True),
        StructField("status", StringType(), True)
    ])
    return schema


def load_trips(
    spark: SparkSession,
    path: str,
    schema: StructType
) -> DataFrame:
    df = spark.read.csv(
        path,
        header=True,
        schema=schema
    )
    return df


def parse_trip_date(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "trip_date",
        to_date("trip_date")
    )
    return df


def add_trip_duration_min(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "trip_duration_min",
        round(
            (unix_timestamp("end_ts") -
             unix_timestamp("start_ts")) / 60
        ).cast("int")
    )
    return df


def add_cost_per_km(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "cost_per_km",
        col("fare") / col("distance_km")
    )
    return df


def filter_peak_hour_trips(df: DataFrame) -> DataFrame:
    df = df.filter(
        hour("start_ts").between(7, 10) |
        hour("start_ts").between(17, 20)
    )
    return df


def top_n_users_by_distance(
    df: DataFrame,
    n: int
) -> DataFrame:

    df = (
        df.groupBy("user_id")
        .agg(
            sum("distance_km").alias("total_distance_km")
        )
        .orderBy(
            col("total_distance_km").desc(),
            col("user_id").asc()
        )
        .limit(n)
    )

    return df


def avg_speed_by_zone(df: DataFrame) -> DataFrame:

    df = (
        df.groupBy("zone")
        .agg(
            avg("speed_kmh").alias("avg_speed_kmh")
        )
    )

    return df


def most_common_vehicle_type(df: DataFrame) -> str:

    result = (
        df.groupBy("vehicle_type")
        .count()
        .orderBy(
            col("count").desc(),
            col("vehicle_type").asc()
        )
        .limit(1)
        .collect()
    )

    if not result:
        return ""

    return result[0]["vehicle_type"]


def cancellation_rate_by_zone(df: DataFrame) -> DataFrame:

    df = (
        df.groupBy("zone")
        .agg(
            count("*").alias("total"),
            sum(
                when(
                    col("status") == "Cancelled",
                    1
                ).otherwise(0)
            ).alias("cancelled")
        )
        .withColumn(
            "cancellation_rate",
            col("cancelled") / col("total")
        )
        .select(
            "zone",
            "cancellation_rate"
        )
    )

    return df


def high_duration_trips(
    df: DataFrame,
    threshold_min: int
) -> DataFrame:

    if "trip_duration_min" not in df.columns:
        df = df.withColumn(
            "trip_duration_min",
            round(
                (unix_timestamp("end_ts") -
                 unix_timestamp("start_ts")) / 60
            ).cast("int")
        )

    return df.filter(
        col("trip_duration_min") > threshold_min
    )


def count_active_vehicles_by_zone(
    vehicles_df: DataFrame
) -> DataFrame:

    df = (
        vehicles_df
        .filter(col("status") == "Active")
        .groupBy("home_zone")
        .agg(
            countDistinct("vehicle_id").alias(
                "active_vehicles"
            )
        )
        .withColumnRenamed(
            "home_zone",
            "zone"
        )
    )

    return df


def daily_net_revenue_trend(df: DataFrame) -> DataFrame:

    df = (
        df.withColumn(
            "net_revenue",
            col("fare") - col("discount")
        )
        .groupBy("trip_date")
        .agg(
            sum("net_revenue").alias(
                "daily_net_revenue"
            )
        )
        .orderBy(
            col("trip_date").asc()
        )
    )

    return df


def top_vehicle_by_net_revenue(
    df: DataFrame
) -> Tuple[str, float]:

    result = (
        df.withColumn(
            "net_revenue",
            col("fare") - col("discount")
        )
        .groupBy("vehicle_id")
        .agg(
            sum("net_revenue").alias(
                "total_net_revenue"
            )
        )
        .orderBy(
            col("total_net_revenue").desc(),
            col("vehicle_id").asc()
        )
        .limit(1)
        .collect()
    )

    if not result:
        return ("", 0.0)

    return (
        result[0]["vehicle_id"],
        float(result[0]["total_net_revenue"])
    )


def list_zones(df: DataFrame) -> List[str]:

    zones_df = (
        df.select("zone")
        .distinct()
        .collect()
    )

    zone_list = []

    for row in zones_df:
        zone_list.append(str(row["zone"]))

    zone_list.sort()

    return zone_list


def trips_in_date_range(
    df: DataFrame,
    start: str,
    end: str
) -> DataFrame:

    df = df.filter(
        to_date(col("trip_date")).between(
            to_date(lit(start)),
            to_date(lit(end))
        )
    )

    return df


def flag_safety_risk(
    df: DataFrame,
    speed_threshold: float,
    brake_threshold: int
) -> DataFrame:

    df = df.withColumn(
        "is_risky",
        (col("speed_kmh") > speed_threshold) |
        (col("harsh_brake_count") > brake_threshold)
    )

    return df


def top_n_risky_vehicles(
    df: DataFrame,
    n: int
) -> DataFrame:

    df = (
        df.filter(col("is_risky") == True)
        .groupBy("vehicle_id")
        .agg(
            count("*").alias("risky_count")
        )
        .orderBy(
            col("risky_count").desc(),
            col("vehicle_id").asc()
        )
        .limit(n)
    )

    return df


def avg_fare_by_payment_method(
    df: DataFrame
) -> DataFrame:

    df = (
        df.groupBy("payment_method")
        .agg(
            avg("fare").alias("avg_fare")
        )
    )

    return df


def get_longest_trip(
    df: DataFrame
) -> Tuple[str, int]:

    if "trip_duration_min" not in df.columns:
        df = df.withColumn(
            "trip_duration_min",
            round(
                (unix_timestamp("end_ts") -
                 unix_timestamp("start_ts")) / 60
            ).cast("int")
        )

    result = (
        df.select(
            "trip_id",
            "trip_duration_min"
        )
        .orderBy(
            col("trip_duration_min").desc(),
            col("trip_id").asc()
        )
        .limit(1)
        .collect()
    )

    if not result:
        return ("", 0)

    return (
        result[0]["trip_id"],
        result[0]["trip_duration_min"]
    )


# ============================================================
# B) Inline Join + Enrichment
# ============================================================

def define_user_profile_schema() -> StructType:

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("plan_id", StringType(), True)
    ])

    return schema


def define_vehicle_schema() -> StructType:

    schema = StructType([
        StructField("plan_id", StringType(), True),
        StructField("plan_name", StringType(), True)
    ])

    return schema


def load_inline_profiles_and_plans(
    spark: SparkSession,
    profile_schema: StructType,
    plan_schema: StructType,
) -> Tuple[DataFrame, DataFrame]:

    profiles_rows = [
        ("U1", "Aarav", "Sharma", None, "P2"),
        ("U2", "Diya", "Iyer", "Diya Iyer", "P1"),
        ("U3", "Kabir", "Singh", None, "P1"),
        ("U4", "Meera", "Nair", None, "P2")
    ]

    plans_rows = [
        ("P1", "Standard"),
        ("P2", "Premium")
    ]

    profiles_df = spark.createDataFrame(
        profiles_rows,
        schema=profile_schema
    )

    plans_df = spark.createDataFrame(
        plans_rows,
        schema=plan_schema
    )

    return profiles_df, plans_df


def join_profiles_with_plans(
    profiles_df: DataFrame,
    plans_df: DataFrame
) -> DataFrame:

    df = (
        profiles_df
        .join(
            plans_df,
            "plan_id",
            "left"
        )
        .select(
            "user_id",
            "first_name",
            "last_name",
            "full_name",
            "plan_id",
            "plan_name"
        )
    )

    return df


def enrich_full_name(
    profile_joined_df: DataFrame
) -> DataFrame:

    df = profile_joined_df.withColumn(
        "full_name",
        when(
            col("full_name").isNull() |
            (trim(col("full_name")) == ""),
            concat_ws(
                col("first_name"),
                col("last_name")
            )
        ).otherwise(
            col("full_name")
        )
    )

    return df


# ============================================================
# C) Unix timestamp conversions
# ============================================================

def add_start_epoch_seconds(
    df: DataFrame
) -> DataFrame:

    df = df.withColumn(
        "start_epoch_seconds",
        unix_timestamp("start_ts").cast("long")
    )

    return df


def add_start_ts_from_epoch(
    df: DataFrame
) -> DataFrame:

    df = df.withColumn(
        "start_ts_from_epoch",
        from_unixtime(
            "start_epoch_seconds"
        ).cast("timestamp")
    )

    return df


# ============================================================
# D) Additional analytics
# ============================================================

def revenue_share_by_vehicle_type(
    df: DataFrame
) -> List[str]:

    result = (
        df.groupBy("vehicle_type")
        .agg(
            sum("rev").alias("total_rev")
        )
    )

    total = (
        result
        .agg(
            sum("total_rev").alias("grand_total")
        )
        .collect()[0]["grand_total"]
    )

    if total is None or total == 0:
        return []

    result = (
        result
        .withColumn(
            "share",
            col("total_rev") / lit(total)
        )
        .orderBy(
            col("share").desc(),
            col("vehicle_type").asc()
        )
        .collect()
    )

    vehicle_list = []

    for row in result:
        vehicle_list.append(
            row["vehicle_type"]
        )

    return vehicle_list


def busiest_zone_by_trips(
    df: DataFrame
) -> Tuple[str, int]:

    result = (
        df.groupBy("zone")
        .count()
        .orderBy(
            col("count").desc(),
            col("zone").asc()
        )
        .limit(1)
        .collect()
    )

    if not result:
        return ("", 0)

    return (
        result[0]["zone"],
        int(result[0]["count"])
    )


def completed_trips_by_zone(
    df: DataFrame
) -> DataFrame:

    df = (
        df.filter(
            col("status") == "Completed"
        )
        .groupBy("zone")
        .agg(
            count("*").alias(
                "completed_trips"
            )
        )
        .orderBy(
            col("completed_trips").desc(),
            col("zone").asc()
        )
    )

    return df


def add_utilization_score(
    df: DataFrame
) -> DataFrame:

    # if "trip_duration_min" not in df.columns:
    #     df = df.withColumn(
    #         "trip_duration_min",
    #         round(
    #             (unix_timestamp("end_ts") -
    #              unix_timestamp("start_ts")) / 60
    #         ).cast("int")
    #     )

    df = df.withColumn(
        "utilization_score",
        col("distance_km") *
        col("trip_duration_min")
    )

    return df


def top_n_users_by_utilization(
    df: DataFrame,
    n: int
) -> DataFrame:

    if "utilization_score" not in df.columns:
        df = add_utilization_score(df)

    df = (
        df.groupBy("user_id")
        .agg(
            sum("utilization_score").alias(
                "total_utilization"
            )
        )
        .orderBy(
            col("total_utilization").desc(),
            col("user_id").asc()
        )
        .limit(n)
    )

    return df


def median_duration_by_zone(
    df: DataFrame
) -> DataFrame:
    df = (
        df.groupBy("zone")
        .agg(
            percentile_approx(
                "trip_duration_min",
                0.5
            ).alias("median_duration_min")
        )
    )

    return df


def minmax_normalize_fare(
    df: DataFrame
) -> DataFrame:

    result = (
        df.agg(
            min("fare").alias("min_fare"),
            max("fare").alias("max_fare")
        )
        .collect()[0]
    )

    min_fare = result["min_fare"]
    max_fare = result["max_fare"]

    if (
        min_fare is None or
        max_fare is None or
        max_fare == min_fare
    ):
        return df.withColumn(
            "fare_norm",
            lit(0.0)
        )

    df = df.withColumn(
        "fare_norm",
        (
            col("fare") - lit(min_fare)
        ) / (
            lit(max_fare) - lit(min_fare)
        )
    )

    return df


def detect_outlier_fares(
    df: DataFrame
) -> DataFrame:

    result = (
        df.agg(
            avg("fare").alias("mean_fare"),
            stddev_pop("fare").alias("std_fare")
        )
        .collect()[0]
    )

    mean_fare = result["mean_fare"]
    std_fare = result["std_fare"]

    if (
        mean_fare is None or
        std_fare is None
    ):
        return df.withColumn(
            "is_outlier",
            lit(False)
        )

    threshold = mean_fare + (2 * std_fare)

    df = df.withColumn(
        "is_outlier",
        col("fare") > lit(threshold)
    )

    return df


def monthly_net_revenue_by_zone(
    df: DataFrame
) -> DataFrame:

    df = df.withColumn(
        "trip_date",
        to_date("trip_date")
    )

    df = df.withColumn(
        "net_revenue",
        col("fare") - col("discount")
    )

    df = df.withColumn(
        "month",
        date_format(
            col("trip_date"),
            "yyyy-MM"
        )
    )

    df = (
        df.groupBy(
            "month",
            "zone"
        )
        .agg(
            sum("net_revenue").alias(
                "monthly_net_revenue"
            )
        )
        .orderBy(
            col("month").asc(),
            col("zone").asc()
        )
    )

    return df


def weekday_peak_trip_counts(
    df: DataFrame
) -> DataFrame:

    df = df.filter(
        hour("start_ts").between(7, 10) |
        hour("start_ts").between(17, 20)
    )

    df = df.withColumn(
        "weekday",
        date_format(
            to_date("trip_date"),
            "E"
        )
    )

    df = (
        df.groupBy("weekday")
        .agg(
            count("*").alias(
                "peak_trips"
            )
        )
    )

    return df


def pivot_payment_counts_by_zone(
    df: DataFrame
) -> DataFrame:

    df = (
        df.groupBy("zone")
        .pivot("payment_method")
        .count()
        .fillna(0)
    )

    return df


def calculate_net_revenue(
    df: DataFrame
) -> DataFrame:

    df = df.withColumn(
        "net_revenue",
        col("fare") - col("discount")
    )

    return df


def flag_service_due(
    vehicles_df: DataFrame,
    due_days: int
) -> DataFrame:

    df = vehicles_df.withColumn(
        "days_since_service",
        datediff(
            current_date(),
            to_date("last_service_date")
        )
    )

    df = df.withColumn(
        "is_service_due",
        col("days_since_service") > due_days
    )

    return df
