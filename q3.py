import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, BooleanType
from pyspark.sql.functions import udf

from functools import *

hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
sc = spark.sparkContext

hdfs_nn = "localhost"
df = spark.read.option("header",True).csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))



def each_rec(rec):
    id = rec['ID_TA']
    reviews_dates = eval(rec['Reviews'])
    reviews = reviews_dates[0]
    dates = reviews_dates[1]
    return [ [id, r, d] for (r,d) in zip(reviews,dates) ]

df.rdd.flatMap(each_rec).toDF(("ID_TA", "review", "date")).write.save("hdfs://%s:9000/assignment2/output/question3/" % (hdfs_nn), format="csv")
