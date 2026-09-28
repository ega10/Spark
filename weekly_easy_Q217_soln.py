from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

def load_university_data(spark:SparkSession,path1:str,path2:str)->DataFrame:
    df1=spark.read.csv(path1,header=True,inferSchema=True)
    df2=spark.read.csv(path2,header=True,inferSchema=True)
    df=(df1.withColumn("user_id",col("user_id").cast("int"))
        .withColumn("user_id",col("user_id").cast("int")))
    return df1,df2
def join_student_data(df1:DataFrame,df2:DataFrame)->DataFrame:
    df=df1.join(df2,on="user_id",how="inner")
    return df
def enrich_full_name(df:DataFrame)->DataFrame:
    df=df.withColumn("full_name",
                     when(col("full_name").isNull(),
                          coalesce(concat_ws(" ","first_name","last_name")))
                     .otherwise(col("full_name")))
    return df
def find_students(df:DataFrame)->DataFrame:
    df=(df.filter(col("grade")=='F')
        .select("user_id","full_name","grade","major"))
    return df

