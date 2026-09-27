from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
def load_bookings_data(spark:SparkSession,path:str)->DataFrame:
    df=(spark.read.csv(path,header=True,inferSchema=True)
        .withColumn("show_date",to_date("show_date"))
    )
    return df
def load_movie_info(spark:SparkSession,path:str)->DataFrame:
    df=spark.read.csv(path,header=True,inferSchema=True)
    return df
def filter_valid_bookings(df:DataFrame)->DataFrame:
    df=df.filter(
        (col("seats_booked")>=0)
        &(col("show_duration_min")>=0))
    return df
def with_overbooking_flag(df:DataFrame)->DataFrame:
    df=df.withColumn("overbooked",
                     when(col("seats_booked")>col("total_seats"),1)
                     .otherwise(0))
    return df
def join_movie_info(bookings_df:DataFrame,info_df:DataFrame)->DataFrame:
    df=bookings_df.join(info_df,on="movie_id",how="left")
    return df
def movie_occupancy_efficiency(df:DataFrame)->str:
    g_df=(
        df.groupBy("movie_id","total_seats")
        .agg(sum("seats_booked").alias("total_booked"))
    )
    e_df=(
        g_df.withColumn("efficiency",
                      when(col("total_seats")==0,0.0).otherwise(col("total_booked")/col("total_seats")))                     
    )
    result=(e_df.orderBy(col("efficiency").desc())
            .select("movie_id")
            .first()
    )
    return str(result["movie_id"])
