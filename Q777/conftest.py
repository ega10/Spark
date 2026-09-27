import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, unix_timestamp, lit, floor

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("pyspark-assessment")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    return spark

@pytest.fixture(scope="session")
def sample_appointments_raw(spark):
    schema = StructType([
        StructField("appointment_id", StringType(), False),
        StructField("patient_id", StringType(), False),
        StructField("doctor", StringType(), False),
        StructField("department", StringType(), False),
        StructField("scheduled_time", StringType(), False),
        StructField("actual_time", StringType(), False),
        StructField("wait_reason", StringType(), False),
    ])
    data = [
        ("A1001","P01","Dr. Rao","Cardiology","2025-01-01 09:00:00","2025-01-01 09:12:00","Late arrival"),   # 12
        ("A1002","P02","Dr. Rao","Cardiology","2025-01-01 10:00:00","2025-01-01 10:55:00","Overbooked"),     # 55
        ("A1003","P03","Dr. Sen","Dermatology","2025-01-01 09:30:00","2025-01-01 09:50:00","System delay"),  # 20
        ("A1004","P01","Dr. Sen","Dermatology","2025-01-02 11:00:00","2025-01-02 11:05:00","Late arrival"),  # 5
        ("A1005","P04","Dr. Iyer","Orthopedics","2025-01-03 08:00:00","2025-01-03 09:10:00","Emergency case"), # 70
    ]
    return spark.createDataFrame(data, schema)

@pytest.fixture(scope="session")
def sample_threshold_minutes():
    return 50

@pytest.fixture(scope="session")
def sample_top_n():
    return 2

def build_wait_minutes_df(df):
    sched = unix_timestamp(col("scheduled_time"))
    actual = unix_timestamp(col("actual_time"))
    wait = floor((actual - sched) / lit(60.0)).cast("int")
    return df.withColumn("wait_minutes", wait)

@pytest.fixture(scope="session")
def sample_appointments_wait_minutes(sample_appointments_raw):
    return build_wait_minutes_df(sample_appointments_raw)
