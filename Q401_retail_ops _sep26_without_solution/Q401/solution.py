from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, to_date, datediff, when, sum as fsum, avg as favg, count


def define_schema() -> StructType:
    return StructType([
        StructField("txn_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("region", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("category", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount_rate", DoubleType(), True),
        StructField("returned", StringType(), True),
        StructField("ship_date", StringType(), True),
        StructField("delivery_date", StringType(), True),
    ])


def load_data(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    return spark.read.option("header", True).schema(schema).csv(path)


def parse_dates(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("order_date", to_date(col("order_date")))
          .withColumn("ship_date", to_date(col("ship_date")))
          .withColumn("delivery_date", to_date(col("delivery_date")))
    )


def add_gross_amount(df: DataFrame) -> DataFrame:
    return df.withColumn("gross_amount", col("quantity") * col("unit_price"))


def add_net_amount(df: DataFrame) -> DataFrame:
    

def add_delivery_days(df: DataFrame) -> DataFrame:
    


def flag_on_time_delivery(df: DataFrame, max_days: int) -> DataFrame:
    
#write other functions from scratch based on LLD instructions 
