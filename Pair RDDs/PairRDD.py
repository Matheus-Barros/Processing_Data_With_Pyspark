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


#transformation combines values with the same key

regularRDD = sc.parallelize([("Messi", 23), ("Ronaldo", 34),("Neymar", 22), ("Messi", 24)])
pairRDD_reducebykey = regularRDD.reduceByKey(lambda x,y : x + y)
reducebykey = pairRDD_reducebykey.collect()

print(reducebykey)

#operation orders pair RDD by key

pairRDD_reducebykey_rev = pairRDD_reducebykey.map(lambda x: (x[1], x[0]))
pairRDD_reducebykey_rev = pairRDD_reducebykey_rev.sortByKey(ascending=True).collect()
print(pairRDD_reducebykey_rev)

#groups all the values with the same key in the pair RDD

airports = [("US","JFK"),("UK","LHR"),("FR","CDG"),("US","SFO")]
regularRDD = sc.parallelize(airports)
pairRDD_group = regularRDD.groupByKey().collect()

for cont, air in pairRDD_group:
    print(cont, list(air))