import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, BooleanType
from pyspark.sql.functions import udf

from functools import *


hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
sc = spark.sparkContext

df = spark.read.option("header",True).csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

def each_grp(records):
    best = reduce(lambda r1,r2: r1 if float(r1['Rating']) > float(r2['Rating']) else r2, records)
    worst = reduce(lambda r1,r2: r2 if float(r1['Rating']) > float(r2['Rating']) else r1, records)
    return [best, worst]


df.filter(df['Rating'] != 'null').rdd.groupBy(lambda row:(row['City'],row['Price Range'])).mapValues(each_grp).flatMap(lambda x:x[1]).toDF().write.save("hdfs://%s:9000/assignment2/output/question2/" % (hdfs_nn), format="csv")