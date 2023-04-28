import sys 
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, BooleanType
from pyspark.sql.functions import udf

from functools import *
import itertools

hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
sc = spark.sparkContext


df = spark.read.option("header",True).parquet("hdfs://%s:9000/assignment2/part2/input/" % (hdfs_nn))

def addCast(d,c):
    d[c['id']] = c
    return d


def for_each_row(row):
    movie_id = row['movie_id']
    title = row['title']
    cast = eval(row['cast'])
    
    actors_dict = {}
    for c in cast:
        actors_dict[c['id']] = c
    
    actors = sorted([ {'id':r['id'], 'name':r['name'] } for r in list(actors_dict.values()) ], key=lambda d:d['id'])
    
    comb_actors = combination(actors,2) 
    return [ ((comb_actor[0]['id'], comb_actor[1]['id']), {'actor_1':comb_actor[0]['name'], 'actor_2':comb_actor[1]['name'],  'title':title, 'movie_id': movie_id} ) for comb_actor in comb_actors ]
    
def combination(l,r):
    return list(itertools.combinations(l, r))
    

def for_each_grp(l):
    if len(l) < 2:
        return []
    else:
        return [ (e['movie_id'], e['title'], e['actor_1'], e['actor_2']) for e in l ]
        
    
result = df.rdd.flatMap(for_each_row).groupByKey().mapValues(for_each_grp).flatMap(lambda x:x[1]).toDF(('movie_id', 'title', 'actor1', 'actor2'))

result.write.save("hdfs://%s:9000/assignment2/output/question5/" % (hdfs_nn), format="parquet")
