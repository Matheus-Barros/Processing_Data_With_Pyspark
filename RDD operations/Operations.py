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

RDD = sc.parallelize([1,2,3,4])
RDD_map = RDD.map(lambda x: x * x)
RDD_map = RDD_map.collect()

print(RDD_map)

RDD1 = sc.parallelize([1,2,3,4])
RDD_filter = RDD1.filter(lambda x: x>2)
RDD_filter = RDD_filter.collect()

print(RDD_filter)

RDD2 = sc.parallelize(["hello world","how are you"])

#transformation returns multiple values for each element in the original RDD
RDD_flatmap = RDD2.flatMap(lambda x: x.split(" "))

#returns an array with the rst N elements of the dataset
print(RDD_flatmap.take(2))

#return the number of elements in the RDD
print(RDD_flatmap.count())

#prints the rst element of the RDD
print(RDD_flatmap.first())

#return all the elements of the dataset as an array
RDD_flatmap = RDD_flatmap.collect()
print(RDD_flatmap)