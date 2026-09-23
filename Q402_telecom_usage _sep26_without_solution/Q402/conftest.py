import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("assessment")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
