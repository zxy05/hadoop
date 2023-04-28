import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, BooleanType
from pyspark.sql.functions import udf

from functools import *


spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
sc = spark.sparkContext

hdfs_nn = sys.argv[1]
df = spark.read.option("header",True).csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))


def for_each_record(row):
    id = row['ID_TA']
    city = row['City']
    cuisine_styles = eval(row['Cuisine Style'])
    return [ (id, city, cuisine) for cuisine in cuisine_styles ]
    

df.rdd.flatMap(for_each_record).toDF(('id', 'City', 'Cuisine')).groupBy(['City', 'Cuisine']).count().write.save("hdfs://%s:9000/assignment2/output/question4/" % (hdfs_nn), format="csv")

