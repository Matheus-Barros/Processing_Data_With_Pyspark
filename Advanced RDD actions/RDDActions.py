'''
@Author: Matheus Barros
Date: 23/04/2021

'''

from pyspark.context import SparkContext, SparkConf
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession

#PARALLELIZING WITH 2 CORES
conf = SparkConf().setAppName("rdd basic").setMaster("local[2]")

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = SparkSession(sc)

#This action is used for aggregating the elements of a regular RDD
x = [1,3,4,6]
RDD = sc.parallelize(x)
reduced = RDD.reduce(lambda x, y : x + y)

print(reduced)

#This action saves RDD into a text file inside a directory with each partition as a separate file
RDD.saveAsTextFile("tempFiles")

#This method can be used to save RDD as a single text le
RDD.coalesce(1).saveAsTextFile("tempFileUnique")

#The last 2 ones, are only available for Pair RDDs
#This action counts the number of elements for each key
rdd = sc.parallelize([("a", 20), ("b", 11), ("a", 1)])

for key, val in rdd.countByKey().items():
    print(key, val)

#collectAsMap() return the key-value pairs in the RDD as a dictionary
collect = sc.parallelize([(1, 2), (3, 4)]).collectAsMap()
print(collect)