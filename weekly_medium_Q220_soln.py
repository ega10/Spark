from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType
from typing import Tuple
def define_movie_schema()->StructType:
    schema=StructType([
        StructField("movie_id",IntegerType(),True),
        StructField("title",StringType(),True),
        StructField("genre",StringType(),True),
        StructField("release_date",StringType(),True),
        StructField("revenue_million",DoubleType(),True)
    ])
    return schema
def load_movies_data(spark:SparkSession,path:str,schema:StructType)->DataFrame:
    df=(spark.read.csv(path,header=True,schema=schema)
        .withColumn("release_date",to_date("release_date"))
    )
    return df
def filter_by_date_range(df:DataFrame,start:str,end:str)->DataFrame:
    df=df.withColumn("release_date",to_date("release_date"))
    df=(df.filter(
        col("release_date").between(
            to_date(lit(start)), 
            to_date(lit(end))
            )))
    return df
def total_revenue_by_genre(df:DataFrame)->DataFrame:
    df=(df.groupBy("genre")
        .agg(sum("revenue_million").alias("total_revenue"))
    )
    return df
def compute_days_since_release(df:DataFrame)->DataFrame:
    df=df.withColumn("days_since_release",date_diff(current_date(),"release_date"))
    return df
def get_highest_grossing_movie(df:DataFrame)->Tuple[str,float]:
    df=(df.select("title","revenue_million")
        .orderBy(col("revenue_million").desc())
        .limit(1)
        .collect()
    )
    return (df[0]["title"],df[0]["revenue_million"])
