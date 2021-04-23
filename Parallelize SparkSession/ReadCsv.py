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

df = sqlContext.read.csv('netflix_titles.csv',header=True)

df.show()