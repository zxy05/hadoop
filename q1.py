import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, BooleanType
from pyspark.sql.functions import udf


hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
sc = spark.sparkContext

df = spark.read.option("header",True).csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

def no_review(reviews):
    return reviews is None or len(reviews) == 0
    
no_review_bool = udf(no_review, BooleanType())
spark.udf.register("no_review_bool", no_review_bool)

df[(df['Rating'] != 'null') & (df['Rating'] >= 1.0) & ~(no_review_bool(df['Reviews']))].write.save("hdfs://%s:9000/assignment2/output/question1/" % (hdfs_nn), format="csv")
