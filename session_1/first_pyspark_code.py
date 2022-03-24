from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.mllib.random import RandomRDDs



# Create SparkSession
spark = SparkSession.builder.appName("asheesh").getOrCreate()

# Transformation
def generate_random_uniform_df(nrows=10, ncols=10,numPartitions=10):
    return RandomRDDs.uniformVectorRDD(spark.sparkContext, nrows,ncols,numPartitions).map(lambda a : a.tolist()).toDF()

df=generate_random_uniform_df(nrows=100000000,ncols=1,numPartitions=100)
df=df.withColumn("num_str", F.udf(lambda z: int(z*100),IntegerType())(df._1)).select("num_str")
df=df.groupby("num_str").count()

# Action
df.orderBy("num_str").show(100)

