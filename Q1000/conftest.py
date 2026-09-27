import os
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, DateType

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("pyspark-assessment")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

@pytest.fixture(scope="session")
def trip_path():
    return os.path.join(os.path.dirname(__file__), "data", "mobility_trips.csv")

@pytest.fixture(scope="session")
def vehicle_path():
    return os.path.join(os.path.dirname(__file__), "data", "vehicles.csv")

@pytest.fixture(scope="session")
def user_path():
    return os.path.join(os.path.dirname(__file__), "data", "users.csv")

@pytest.fixture(scope="session")
def sample_trips_raw_df(spark, trip_path):
    return spark.read.option("header", True).option("inferSchema", True).csv(trip_path)

@pytest.fixture(scope="session")
def sample_trips_typed_df(sample_trips_raw_df):
    df = sample_trips_raw_df
    # Make sure types are stable for downstream tests (no student dependency)
    return (
        df.withColumn("trip_date", F.to_date(F.col("trip_date")))
          .withColumn("start_ts", F.to_timestamp(F.col("start_ts")))
          .withColumn("end_ts", F.to_timestamp(F.col("end_ts")))
          .withColumn("distance_km", F.col("distance_km").cast("double"))
          .withColumn("fare", F.col("fare").cast("double"))
          .withColumn("discount", F.col("discount").cast("double"))
          .withColumn("speed_kmh", F.col("speed_kmh").cast("double"))
          .withColumn("harsh_brake_count", F.col("harsh_brake_count").cast("int"))
    )

@pytest.fixture(scope="session")
def sample_trips_duration_df(sample_trips_typed_df):
    exp_ts = F.unix_timestamp(F.col("start_ts"))
    end_ts = F.unix_timestamp(F.col("end_ts"))
    dur_min = (end_ts - exp_ts) / F.lit(60.0)
    return sample_trips_typed_df.withColumn("trip_duration_min", F.round(dur_min, 0).cast("int"))

@pytest.fixture(scope="session")
def sample_trips_util_df(sample_trips_duration_df):
    return sample_trips_duration_df.withColumn("utilization_score", (F.col("distance_km") * F.col("trip_duration_min")).cast("double"))

@pytest.fixture(scope="session")
def sample_trips_risk_df(sample_trips_typed_df):
    return sample_trips_typed_df.withColumn(
        "is_risky",
        (F.col("speed_kmh") > F.lit(17.0)) | (F.col("harsh_brake_count") > F.lit(2))
    )

@pytest.fixture(scope="session")
def sample_trips_epoch_df(sample_trips_typed_df):
    return sample_trips_typed_df.withColumn("start_epoch_seconds", F.unix_timestamp(F.col("start_ts")).cast("long"))

@pytest.fixture(scope="session")
def sample_metrics_df(sample_trips_typed_df):
    # Precompute vehicle_type revenue totals (net revenue = fare - discount)
    net = sample_trips_typed_df.withColumn("net_revenue", F.col("fare") - F.col("discount"))
    return net.groupBy("vehicle_type").agg(F.sum("net_revenue").alias("rev"))

@pytest.fixture(scope="session")
def sample_vehicles_typed_df(spark, vehicle_path):
    df = spark.read.option("header", True).option("inferSchema", True).csv(vehicle_path)
    return df.withColumn("last_service_date", F.to_date(F.col("last_service_date")))