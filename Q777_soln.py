from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
def load_appointment_data(spark: SparkSession) -> DataFrame:
    path="data/appointments.csv"
    schema=StructType([
        StructField("appointment_id",StringType(),True),
        StructField("patient_id",StringType(),True),
        StructField("doctor",StringType(),True),
        StructField("department",StringType(),True),
        StructField("scheduled_time",StringType(),True),
        StructField("actual_time",StringType(),True),
        StructField("wait_reason",StringType(),True)

    ])
    df=spark.read.csv(path,header=True,schema=schema)
    return df
def append_wait_minutes(df:DataFrame)->DataFrame:
    df=(df.withColumn("wait_minutes",floor(
            (unix_timestamp("actual_time")-unix_timestamp("scheduled_time"))/60
             ).cast("int"))
    )
    return df
def get_long_wait_appointments(df:DataFrame,threshold_minutes:int)->DataFrame:
    df=df.withColumn("wait_minutes",
                     floor(
                         (unix_timestamp("actual_time")-unix_timestamp("scheduled_time"))/60
                         ).cast("int")
                     )
    df=df.filter(col("wait_minutes")>threshold_minutes)
    return df
def most_delayed_doctor(df_with_wait)->DataFrame:
    df=(df_with_wait.groupBy("doctor")
        .agg(sum("wait_minutes").alias("total_wait"))
        .orderBy(col("total_wait").desc(),
                 col("doctor").asc())
        .select("doctor","total_wait")
        .limit(1)
    )
    return df
def long_wait_percentage(df_with_wait:DataFrame)->float:
    long_wait_count=(df_with_wait.filter(col("wait_minutes")>30)
             .count()
    )
    total_count=df_with_wait.count()
    if total_count==0:
        return 0.0
    else:
        return float(long_wait_count/total_count)*100
def most_delayed_appointment(df_with_wait: DataFrame) -> tuple[str, int]:
    df=(df_with_wait.orderBy(col("wait_minutes").desc(),
                             col("appointment_id").asc()
                            )
                            .limit(1)
                            .collect()
    )
    if not df:
        return ("",0)
    return(df[0]["appointment_id"],df[0]["wait_minutes"])
def avg_wait_by_department(df_with_wait:DataFrame)->DataFrame:
    df=(df_with_wait.groupBy("department")
        .agg(avg("wait_minutes").alias("avg_wait_minutes"))
        .orderBy(col("avg_wait_minutes").desc(),
                 col("department").asc())
        .select("department","avg_wait_minutes")
    )
    return df
def top_n_patients_by_wait(df_with_wait:DataFrame,n:int)->DataFrame:
    df=(df_with_wait.groupBy("patient_id")
        .agg(sum("wait_minutes").alias("total_wait_minutes"))
        .orderBy(col("total_wait_minutes").desc(),
                 col("patient_id").asc())
        .limit(n)
    )
    return df
def wait_reason_counts(df:DataFrame)->DataFrame:
    df=(df.groupBy("wait_reason")
        .agg(count("*").alias("reason_count"))
        .orderBy(col("reason_count").desc(),
                 col("wait_reason").asc())
        .select("wait_reason","reason_count")
        
    )
    return df
